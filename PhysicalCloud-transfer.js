// @ts-check
// PhysicalCloud-transfer.js — Etapa 3: Transferência P2P de arquivos
//
// Protocolo sobre DataChannel (binário + JSON):
//
//   REMETENTE                          RECEPTOR
//   ─────────────────────────────────────────────
//   JSON {type:"file-offer", ...}  →
//                                  ←  JSON {type:"file-accept", transferId}
//   ArrayBuffer chunk 1            →
//   ArrayBuffer chunk 2            →
//   ...
//   JSON {type:"file-done", transferId, sha256}  →
//                                  ←  JSON {type:"file-ok", transferId}   (sucesso)
//                                  ←  JSON {type:"file-error", transferId, motivo} (falha)
//
//   Qualquer lado pode cancelar com:
//   JSON {type:"file-cancel", transferId}

const TAMANHO_CHUNK = 256 * 1024;
const BUFFER_ALTO   = 4 * 1024 * 1024;
const BUFFER_BAIXO  = 512 * 1024;

if (!(/** @type {any} */ (window).PhysicalCloud)) {
    console.error("PhysicalCloud-transfer.js: window.PhysicalCloud não encontrado.");
}

/**
 * @typedef {{
 *   transferId: string,
 *   sessaoId: string,
 *   nome: string,
 *   tamanho: number,
 *   tipo: string,
 *   enviados: number,
 *   estado: "enviando"|"recebendo"|"concluido"|"cancelado"|"erro",
 *   cancelarPausa?: (() => void) | null,
 * }} EntradaTransfer
 */

/** @type {Map<string, RTCDataChannel>} */
const canais = new Map();
/** @type {Map<string, EntradaTransfer>} */
const transferencias = new Map();
/** @type {Map<string, File[]>} */
const filas = new Map();
/** @type {Map<string, string>} */
const ativasEnvio = new Map();
/** @type {Map<string, string>} */
const ativasRecebimento = new Map();
/** @type {Map<string, { fila: Array<() => Promise<void>>, ocupado: boolean, cancelado: boolean }>} */
const controladoresFila = new Map();

// ─── Helper de cleanup idempotente ───────────────────────────────────────────
// [Fix.7] Garante que apenas o dono do transferId apaga a entrada de ativasEnvio.
// Dois cleanups concorrentes (enviarArquivo + file-cancel) viram no-op no segundo.
/**
 * @param {string} sessaoId
 * @param {string} transferId
 */
function limparSeProprio(sessaoId, transferId) {
    if (transferencias.has(transferId)) transferencias.delete(transferId);
    if (ativasEnvio.get(sessaoId) === transferId) ativasEnvio.delete(sessaoId);
}

// ─── SHA-256 via Web Crypto ───────────────────────────────────────────────────

/**
 * @param {ArrayBuffer} buffer
 * @returns {Promise<string>}
 */
async function calcularSha256(buffer) {
    const hash = await crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
}

function gerarTransferId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const b = new Uint8Array(16);
    crypto.getRandomValues(b);
    return Array.from(b, x => x.toString(16).padStart(2, "0")).join("");
}

/**
 * @param {string} tipo
 * @param {object} detalhe
 */
function emitir(tipo, detalhe) {
    window.dispatchEvent(new CustomEvent(tipo, { detail: detalhe }));
}

// ─── Envio ────────────────────────────────────────────────────────────────────

/**
 * @param {string} sessaoId
 * @param {File} arquivo
 */
async function enviarArquivo(sessaoId, arquivo) {
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") {
        emitir("physicalcloud:transfer-erro", { sessaoId, motivo: "Canal não está aberto." });
        return;
    }

    const transferId = gerarTransferId();
    /** @type {EntradaTransfer} */
    const entrada = {
        transferId, sessaoId,
        nome: arquivo.name,
        tamanho: arquivo.size,
        tipo: arquivo.type || "application/octet-stream",
        enviados: 0,
        estado: "enviando",
        cancelarPausa: null,
    };
    transferencias.set(transferId, entrada);
    ativasEnvio.set(sessaoId, transferId);

    try {
        dc.send(JSON.stringify({
            type: "file-offer",
            transferId,
            nome: arquivo.name,
            tamanho: arquivo.size,
            tipo: arquivo.type || "application/octet-stream",
        }));
    } catch (e) {
        console.error("Falha ao enviar file-offer:", e);
        limparSeProprio(sessaoId, transferId);
        processarFila(sessaoId);
        return;
    }

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, nome: arquivo.name, tamanho: arquivo.size, direcao: "envio",
    });

    // Aguarda aceitação
    try {
        await aguardarAceitacao(transferId, dc);
    } catch {
        entrada.estado = "cancelado";
        limparSeProprio(sessaoId, transferId);
        emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
        processarFila(sessaoId);
        return;
    }

    // ── Streaming com backpressure ────────────────────────────────────────────
    const reader = arquivo.stream().getReader();
    dc.bufferedAmountLowThreshold = BUFFER_BAIXO;
    let pausado = false;
    /** @type {((value?: any) => void) | null} */
    let resolverPausa = null;

    const liberarPausa = () => {
        if (pausado && resolverPausa) {
            const r = resolverPausa;
            pausado = false;
            resolverPausa = null;
            r();
        }
    };
    const onBufferLow = () => liberarPausa();
    const onClose = () => liberarPausa();

    dc.addEventListener("bufferedamountlow", onBufferLow);
    dc.addEventListener("close", onClose);
    entrada.cancelarPausa = liberarPausa;

    try {
        while (true) {
            if (/** @type {string} */ (entrada.estado) === "cancelado") break;

            const { done, value } = await reader.read();
            if (done) break;

            let offset = 0;
            while (offset < value.byteLength) {
                if (/** @type {string} */ (entrada.estado) === "cancelado") break;
                if (dc.readyState !== "open") { entrada.estado = "cancelado"; break; }

                const fim = Math.min(offset + TAMANHO_CHUNK, value.byteLength);
                const fatia = value.buffer.slice(value.byteOffset + offset, value.byteOffset + fim);

                try {
                    dc.send(fatia);
                } catch {
                    entrada.estado = "cancelado";
                    break;
                }
                entrada.enviados += fatia.byteLength;

                emitir("physicalcloud:transfer-progresso", {
                    transferId, sessaoId, enviados: entrada.enviados, total: arquivo.size,
                    timestamp: Date.now(),
                });

                if (dc.bufferedAmount > BUFFER_ALTO) {
                    pausado = true;
                    await new Promise((res) => { resolverPausa = res; });
                }
                offset = fim;
            }
        }
    } finally {
        reader.releaseLock();
        dc.removeEventListener("bufferedamountlow", onBufferLow);
        dc.removeEventListener("close", onClose);
        entrada.cancelarPausa = null;
    }

    if (/** @type {string} */ (entrada.estado) === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        processarFila(sessaoId);
        return;
    }

    // ── SHA-256 após streaming ────────────────────────────────────────────────
    let sha256;
    try {
        sha256 = await calcularSha256(await arquivo.arrayBuffer());
    } catch (e) {
        console.error("Falha ao calcular SHA-256:", e);
        entrada.estado = "cancelado";
        try { if (dc.readyState === "open") dc.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* noop */ }
        limparSeProprio(sessaoId, transferId);
        emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
        processarFila(sessaoId);
        return;
    }

    // [Fix.7-A] Cancelamento que chegou durante o SHA — o check pré-SHA já passou.
    // Sem isso, a entrada vaza e o file-done ainda seria enviado.
    if (/** @type {string} */ (entrada.estado) === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        processarFila(sessaoId);
        return;
    }

    // [Fix.7-D] dc pode ter fechado durante o SHA. send() lançaria.
    try {
        dc.send(JSON.stringify({ type: "file-done", transferId, sha256 }));
    } catch (e) {
        console.error("Falha ao enviar file-done:", e);
        limparSeProprio(sessaoId, transferId);
        processarFila(sessaoId);
    }
}

// ─── Aguardar aceitação ───────────────────────────────────────────────────────

/** @type {Map<string, { resolve: () => void, reject: (e: Error) => void }>} */
const aguardandoAceitacaoMap = new Map();

/**
 * @param {string} transferId
 * @param {RTCDataChannel} dc
 * @returns {Promise<void>}
 */
function aguardarAceitacao(transferId, dc) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            aguardandoAceitacaoMap.delete(transferId);
            reject(new Error("Tempo esgotado aguardando aceitação."));
        }, 30000);

        aguardandoAceitacaoMap.set(transferId, {
            resolve: () => { clearTimeout(timeout); resolve(); },
            reject: (e) => { clearTimeout(timeout); reject(e); },
        });

        const onclose = () => {
            aguardandoAceitacaoMap.get(transferId)?.reject(new Error("Canal fechado."));
            aguardandoAceitacaoMap.delete(transferId);
            dc.removeEventListener("close", onclose);
        };
        dc.addEventListener("close", onclose);
    });
}

// ─── Fila de envio ────────────────────────────────────────────────────────────

/** @param {string} sessaoId */
function processarFila(sessaoId) {
    if (ativasEnvio.has(sessaoId)) return;
    const fila = filas.get(sessaoId);
    if (!fila || fila.length === 0) return;
    const proximo = fila.shift();
    if (proximo) {
        // [Fix.7] catch para evitar unhandledRejection
        enviarArquivo(sessaoId, proximo).catch((e) => {
            console.error("Erro em enviarArquivo:", e);
        });
    }
}

/**
 * @param {string} sessaoId
 * @param {File[]} arquivos
 */
function enfileirarArquivos(sessaoId, arquivos) {
    if (!filas.has(sessaoId)) filas.set(sessaoId, []);
    // @ts-ignore
    filas.get(sessaoId).push(...arquivos);
    processarFila(sessaoId);
}

// ─── Recebimento ─────────────────────────────────────────────────────────────

/**
 * @type {Map<string, {
 *   transferId: string, nome: string, tamanho: number, tipo: string,
 *   chunks: ArrayBuffer[], recebidos: number,
 *   writerHandle: FileSystemWritableFileStream | null,
 *   fileHandle: FileSystemFileHandle | null,
 * }>}
 */
const recebimentos = new Map();

/**
 * @param {string} sessaoId
 * @param {any} msg
 * @param {RTCDataChannel} dc
 * @param {(fn: () => Promise<void>) => void} enfileirar
 */
async function tratarFileOffer(sessaoId, msg, dc, enfileirar) {
    const { transferId, nome, tamanho, tipo } = msg;

    if (
        typeof transferId !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(transferId)
    ) return;
    if (typeof nome !== "string" || nome.length === 0 || nome.length > 255) return;
    if (typeof tamanho !== "number" || !Number.isFinite(tamanho) || tamanho < 0) return;

    const tipoSeguro = (typeof tipo === "string" && /^[\w.+-]+\/[\w.+-]+$/.test(tipo))
        ? tipo
        : "application/octet-stream";

    recebimentos.set(sessaoId, {
        transferId, nome, tamanho, tipo: tipoSeguro,
        chunks: [], recebidos: 0,
        writerHandle: null, fileHandle: null,
    });
    ativasRecebimento.set(sessaoId, transferId);

    dc.send(JSON.stringify({ type: "file-accept", transferId }));

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, nome, tamanho, direcao: "recebimento",
    });

    const temFSAPI = "showSaveFilePicker" in window;
    if (temFSAPI) {
        // @ts-ignore
        window.showSaveFilePicker({
            suggestedName: nome,
            types: [{ description: "Arquivo", accept: { [tipoSeguro]: [] } }],
        })
            .then(/** @param {FileSystemFileHandle} fh */ (fh) => {
                enfileirar(async () => {
                    const wh = await fh.createWritable();
                    const rec = recebimentos.get(sessaoId);
                    if (!rec || rec.transferId !== transferId) {
                        await wh.abort().catch(() => {});
                        return;
                    }
                    for (const chunk of rec.chunks) await wh.write(chunk);
                    rec.chunks = [];
                    rec.writerHandle = wh;
                    rec.fileHandle = fh;
                });
            })
            .catch(() => { /* usuário cancelou ou API falhou */ });
    }
}

/**
 * @param {string} sessaoId
 * @param {any} msg
 * @param {RTCDataChannel} dc
 * @param {(fn: () => Promise<void>) => void} enfileirar
 */
async function tratarMensagemJson(sessaoId, msg, dc, enfileirar) {
    if (!msg || typeof msg.type !== "string") return;

    switch (msg.type) {
        case "file-offer":
            await tratarFileOffer(sessaoId, msg, dc, enfileirar);
            break;

        case "file-accept": {
            const prom = aguardandoAceitacaoMap.get(msg.transferId);
            if (prom) {
                aguardandoAceitacaoMap.delete(msg.transferId);
                prom.resolve();
            }
            break;
        }

        case "file-done": {
            const rec = recebimentos.get(sessaoId);
            if (!rec || rec.transferId !== msg.transferId) return;

            const sha256Remoto = msg.sha256;

            if (rec.writerHandle) {
                try {
                    await rec.writerHandle.close();
                } catch (e) {
                    console.error("Erro ao fechar writer:", e);
                    try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha ao salvar arquivo." })); } catch { /* noop */ }
                    recebimentos.delete(sessaoId);
                    ativasRecebimento.delete(sessaoId);
                    return;
                }

                try {
                    if (rec.fileHandle) {
                        const f = await rec.fileHandle.getFile();
                        const buf = await f.arrayBuffer();
                        const sha256Local = await calcularSha256(buf);
                        if (sha256Local !== sha256Remoto) {
                            dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." }));
                            emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, motivo: "Arquivo corrompido (SHA-256 não confere)." });
                            recebimentos.delete(sessaoId);
                            ativasRecebimento.delete(sessaoId);
                            return;
                        }
                    }
                } catch { /* ignora */ }

                dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId }));
                emitir("physicalcloud:transfer-concluido", {
                    transferId: rec.transferId, sessaoId, nome: rec.nome, tamanho: rec.tamanho, direcao: "recebimento",
                });

            } else {
                const blob = new Blob(rec.chunks, { type: rec.tipo || "application/octet-stream" });
                const buf = await blob.arrayBuffer();
                const sha256Local = await calcularSha256(buf);

                if (sha256Local !== sha256Remoto) {
                    dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." }));
                    emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, motivo: "Arquivo corrompido (SHA-256 não confere)." });
                    recebimentos.delete(sessaoId);
                    ativasRecebimento.delete(sessaoId);
                    return;
                }

                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = rec.nome;
                document.body.appendChild(a);
                a.click();
                setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 5000);

                dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId }));
                emitir("physicalcloud:transfer-concluido", {
                    transferId: rec.transferId, sessaoId, nome: rec.nome, tamanho: rec.tamanho, direcao: "recebimento",
                });
            }

            recebimentos.delete(sessaoId);
            ativasRecebimento.delete(sessaoId);
            break;
        }

        case "file-cancel": {
            // Recebemos cancelamento do peer.
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === msg.transferId) {
                if (rec.writerHandle) { try { await rec.writerHandle.abort(); } catch { /* noop */ } }
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
            }

            const tidEnvio = ativasEnvio.get(sessaoId);
            if (tidEnvio && tidEnvio === msg.transferId) {
                const e = transferencias.get(tidEnvio);
                if (e) {
                    e.estado = "cancelado";
                    if (typeof e.cancelarPausa === "function") e.cancelarPausa();
                }
                // [Fix.7] Cleanup aqui para o caso de enviarArquivo já ter retornado
                // (entre file-done e file-ok/file-error). Se enviarArquivo ainda está
                // rodando, seu próprio cleanup é no-op (limparSeProprio).
                limparSeProprio(sessaoId, msg.transferId);
                processarFila(sessaoId);
            }
            emitir("physicalcloud:transfer-cancelado", { sessaoId, transferId: msg.transferId });
            break;
        }

        case "file-ok": {
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const { nome, tamanho } = entrada;
            limparSeProprio(sessaoId, tid);
            emitir("physicalcloud:transfer-concluido", {
                transferId: tid, sessaoId, nome, tamanho, direcao: "envio",
            });
            processarFila(sessaoId);
            break;
        }

        case "file-error": {
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const motivo = msg.motivo || "Receptor reportou erro desconhecido.";
            entrada.estado = "erro";
            limparSeProprio(sessaoId, tid);
            emitir("physicalcloud:transfer-erro", { transferId: tid, sessaoId, motivo });
            processarFila(sessaoId);
            break;
        }
    }
}

/**
 * @param {string} sessaoId
 * @param {ArrayBuffer} data
 * @param {RTCDataChannel} dc
 */
async function tratarChunk(sessaoId, data, dc) {
    const rec = recebimentos.get(sessaoId);
    if (!rec) return;

    if (rec.writerHandle) {
        try {
            await rec.writerHandle.write(data);
        } catch (e) {
            console.error("Erro ao escrever chunk em disco:", e);
            try { if (dc.readyState === "open") dc.send(JSON.stringify({ type: "file-cancel", transferId: rec.transferId })); } catch { /* noop */ }
            await rec.writerHandle.abort().catch(() => {});
            recebimentos.delete(sessaoId);
            ativasRecebimento.delete(sessaoId);
            emitir("physicalcloud:transfer-erro", {
                sessaoId, transferId: rec.transferId, motivo: "Erro ao escrever no disco.",
            });
            return;
        }
    } else {
        rec.chunks.push(data);
    }

    rec.recebidos += data.byteLength;
    emitir("physicalcloud:transfer-progresso", {
        transferId: rec.transferId, sessaoId, enviados: rec.recebidos, total: rec.tamanho,
        timestamp: Date.now(),
    });
}

// ─── Integração com eventos do WebRTC ────────────────────────────────────────

window.addEventListener("physicalcloud:canal-aberto", (event) => {
    const { sessaoId, dc } = /** @type {CustomEvent} */ (event).detail;
    canais.set(sessaoId, dc);

    /** @type {{ fila: Array<() => Promise<void>>, ocupado: boolean, cancelado: boolean }} */
    const ctrl = { fila: [], ocupado: false, cancelado: false };
    controladoresFila.set(sessaoId, ctrl);

    function processar() {
        if (ctrl.cancelado || ctrl.ocupado) return;
        ctrl.ocupado = true;
        (async () => {
            while (!ctrl.cancelado && ctrl.fila.length > 0) {
                const tarefa = ctrl.fila.shift();
                if (tarefa) {
                    try { await tarefa(); } catch (e) { console.error("Erro na fila de transferência:", e); }
                }
            }
            ctrl.ocupado = false;
        })();
    }

    /** @param {() => Promise<void>} fn */
    function enfileirar(fn) {
        if (ctrl.cancelado) return;
        ctrl.fila.push(fn);
        processar();
    }

    dc.addEventListener("message", (/** @type {{ data: string | ArrayBuffer; }} */ ev) => {
        if (typeof ev.data === "string") {
            let msg;
            try { msg = JSON.parse(ev.data); } catch { msg = null; }
            if (msg && typeof msg.type === "string" && msg.type.startsWith("file-")) {
                enfileirar(() => tratarMensagemJson(sessaoId, msg, dc, enfileirar));
            }
        } else if (ev.data instanceof ArrayBuffer) {
            if (ativasRecebimento.has(sessaoId)) {
                enfileirar(() => tratarChunk(sessaoId, /** @type {ArrayBuffer} */ (ev.data), dc));
            }
        }
    });
});

window.addEventListener("physicalcloud:sessao-encerrada", (event) => {
    const { sessaoId } = /** @type {CustomEvent} */ (event).detail;
    canais.delete(sessaoId);
    filas.delete(sessaoId);

    const ctrl = controladoresFila.get(sessaoId);
    if (ctrl) {
        ctrl.cancelado = true;
        ctrl.fila.length = 0;
        controladoresFila.delete(sessaoId);
    }

    const tidEnvio = ativasEnvio.get(sessaoId);
    if (tidEnvio) {
        const entrada = transferencias.get(tidEnvio);
        if (entrada) {
            entrada.estado = "cancelado";
            if (typeof entrada.cancelarPausa === "function") entrada.cancelarPausa();
        }
        // [Fix.7-C] apaga a entrada de transferencias — antes só ativasEnvio era limpo.
        transferencias.delete(tidEnvio);
        ativasEnvio.delete(sessaoId);
    }

    const rec = recebimentos.get(sessaoId);
    if (rec) {
        if (rec.writerHandle) rec.writerHandle.abort().catch(() => {});
        recebimentos.delete(sessaoId);
    }
    ativasRecebimento.delete(sessaoId);
});

// ─── Listener do botão "Enviar arquivo" ──────────────────────────────────────

window.addEventListener("physicalcloud:solicitar-envio", () => {
    if (canais.size === 0) {
        window.dispatchEvent(new CustomEvent("physicalcloud:transfer-sem-canal"));
        return;
    }

    const input = document.createElement("input");
    input.type = "file";
    input.multiple = true;
    input.style.display = "none";
    document.body.appendChild(input);

    const removerInput = () => {
        if (input.parentNode) document.body.removeChild(input);
    };
    input.addEventListener("cancel", removerInput);

    input.addEventListener("change", () => {
        const arquivos = Array.from(input.files || []);
        removerInput();
        if (arquivos.length === 0) return;

        const sessaoIds = Array.from(canais.keys());
        if (sessaoIds.length === 1) {
            enfileirarArquivos(sessaoIds[0], arquivos);
        } else {
            window.dispatchEvent(new CustomEvent("physicalcloud:escolher-destino", {
                detail: { arquivos, sessaoIds },
            }));
        }
    });

    input.click();
});

// ─── API pública ──────────────────────────────────────────────────────────────

/** @type {any} */ (window).PhysicalCloudTransfer = {
    enfileirarArquivos,
    canais,
    transferencias,
    /**
     * @param {string} transferId
     */
    cancelarTransferencia(transferId) {
        if (!transferId) return;

        let sessaoId = null;
        /** @type {"envio"|"recebimento"|null} */
        let sentido = null;

        for (const [sid, tid] of ativasEnvio) {
            if (tid === transferId) { sessaoId = sid; sentido = "envio"; break; }
        }
        if (!sessaoId) {
            for (const [sid, tid] of ativasRecebimento) {
                if (tid === transferId) { sessaoId = sid; sentido = "recebimento"; break; }
            }
        }
        if (!sessaoId || !sentido) return;

        const dc = canais.get(sessaoId);
        if (dc && dc.readyState === "open") {
            try { dc.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* noop */ }
        }

        if (sentido === "envio") {
            const entrada = transferencias.get(transferId);
            if (entrada) {
                entrada.estado = "cancelado";
                if (typeof entrada.cancelarPausa === "function") entrada.cancelarPausa();
            }
            const prom = aguardandoAceitacaoMap.get(transferId);
            if (prom) {
                aguardandoAceitacaoMap.delete(transferId);
                prom.reject(new Error("Cancelado pelo usuário."));
            }
            // [Fix.7] safety net para o caso de enviarArquivo já ter retornado.
            limparSeProprio(sessaoId, transferId);
            processarFila(sessaoId);
        } else {
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === transferId) {
                if (rec.writerHandle) rec.writerHandle.abort().catch(() => {});
                recebimentos.delete(sessaoId);
            }
            ativasRecebimento.delete(sessaoId);
        }

        emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
    },
};