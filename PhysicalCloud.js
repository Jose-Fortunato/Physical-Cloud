// @ts-check
// PhysicalCloud-transfer.js — Etapa 3: Transferência P2P de arquivos

// @ts-ignore
const TAMANHO_CHUNK = 256 * 1024;
const BUFFER_ALTO   = 4 * 1024 * 1024;
const BUFFER_BAIXO  = 512 * 1024;
const TIMEOUT_ACEITE_MS = 30000;   // [Fix.10] timeout do file-accept
const TIMEOUT_OK_MS     = 60000;   // [Fix.12] timeout do file-ok/file-error

if (!(/** @type {any} */ (window).PhysicalCloud)) {
    console.error("PhysicalCloud-transfer.js: window.PhysicalCloud não encontrado.");
}

/**
 * @typedef {{
 *   transferId: string, sessaoId: string, nome: string, tamanho: number, tipo: string,
 *   enviados: number, estado: "enviando"|"recebendo"|"concluido"|"cancelado"|"erro",
 *   cancelarPausa?: (() => void) | null,
 *   cancelamentoEmitido?: boolean,
 * }} EntradaTransfer
 */

/** @type {Map<string, RTCDataChannel>} */ const canais = new Map();
/** @type {Map<string, EntradaTransfer>} */ const transferencias = new Map();
/** @type {Map<string, File[]>} */ const filas = new Map();
/** @type {Map<string, string>} */ const ativasEnvio = new Map();
/** @type {Map<string, string>} */ const ativasRecebimento = new Map();
/** @type {Map<string, { fila: Array<() => Promise<void>>, ocupado: boolean, cancelado: boolean }>} */ const controladoresFila = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * @param {string} sessaoId
 * @param {string | undefined} transferId
 */
function limparSeProprio(sessaoId, transferId) {
    // @ts-ignore
    if (transferencias.has(transferId)) transferencias.delete(transferId);
    if (ativasEnvio.get(sessaoId) === transferId) ativasEnvio.delete(sessaoId);
}

/**
 * @param {{ transferId: any; sessaoId?: string; nome?: string; tamanho?: number; tipo?: string; enviados?: number; estado?: "enviando" | "recebendo" | "concluido" | "cancelado" | "erro"; cancelarPausa?: (() => void) | null | undefined; cancelamentoEmitido: any; }} entrada
 * @param {string} sessaoId
 */
function emitirCancelamento(entrada, sessaoId) {
    if (entrada.cancelamentoEmitido) return;
    entrada.cancelamentoEmitido = true;
    emitir("physicalcloud:transfer-cancelado", { transferId: entrada.transferId, sessaoId });
}

/**
 * @param {BufferSource} buffer
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
 * @param {{ transferId?: any; sessaoId: any; motivo?: any; nome?: any; tamanho?: any; direcao?: string; enviados?: number; total?: any; timestamp?: number; }} detalhe
 */
function emitir(tipo, detalhe) {
    window.dispatchEvent(new CustomEvent(tipo, { detail: detalhe }));
}

// ─── Aguardar aceitação ─────────────────────────────────────────────────────

/** @type {Map<string, { resolve: () => void, reject: (e: Error) => void }>} */
const aguardandoAceitacaoMap = new Map();

/**
 * @param {string} transferId
 * @param {RTCDataChannel} dc
 */
function aguardarAceitacao(transferId, dc) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            aguardandoAceitacaoMap.delete(transferId);
            reject(new Error("Tempo esgotado aguardando aceitação."));
        }, TIMEOUT_ACEITE_MS);

        aguardandoAceitacaoMap.set(transferId, {
            // @ts-ignore
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

/**
 * [Fix.10] Rejeita aguardarAceitacao pendente — chamado por file-cancel, file-error
 * e cleanup de file-ok. Sem isso, enviarArquivo ficava pendurado 30 s após o peer
 * cancelar (file-cancel) ou enviar erro durante a janela de aceitação.
 * @param {string} transferId
 * @param {string} motivo
 */
function rejeitarAceitacaoPendente(transferId, motivo) {
    const prom = aguardandoAceitacaoMap.get(transferId);
    if (prom) {
        aguardandoAceitacaoMap.delete(transferId);
        prom.reject(new Error(motivo));
    }
}

// ─── Timeout de file-ok/file-error ──────────────────────────────────────────

/** @type {Map<string, ReturnType<typeof setTimeout>>} */
const timeoutsOk = new Map();

/**
 * [Fix.12] Arma timeout para file-ok/file-error. Se o peer não responder em
 * 60 s, força erro para não deixar entrada travada em transferencias.
 * @param {string} transferId
 * @param {string} sessaoId
 */
function armarTimeoutOk(transferId, sessaoId) {
    const t = setTimeout(() => {
        timeoutsOk.delete(transferId);
        const entrada = transferencias.get(transferId);
        if (!entrada) return;
        entrada.estado = "erro";
        limparSeProprio(sessaoId, transferId);
        emitir("physicalcloud:transfer-erro", {
            transferId, sessaoId, motivo: "Tempo esgotado aguardando confirmação do peer.",
        });
        processarFila(sessaoId);
    }, TIMEOUT_OK_MS);
    timeoutsOk.set(transferId, t);
}

/**
 * @param {string} transferId
 */
function cancelarTimeoutOk(transferId) {
    const t = timeoutsOk.get(transferId);
    if (t) { clearTimeout(t); timeoutsOk.delete(transferId); }
}

// ─── Envio ──────────────────────────────────────────────────────────────────

/**
 * @param {string} sessaoId
 * @param {File} arquivo
 */
async function enviarArquivo(sessaoId, arquivo) {
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") {
        emitir("physicalcloud:transfer-erro", { sessaoId, motivo: "Canal não está aberto." });
        processarFila(sessaoId);
        return;
    }

    // [Fix.11] Cria reader ANTES de tocar em Maps/UI. Se stream() lançar
    // (arquivo removido do disco, permissão revogada), não deixa estado órfão.
    /** @type {ReadableStreamDefaultReader<Uint8Array> | null} */
    let reader = null;
    try {
        reader = arquivo.stream().getReader();
    } catch (e) {
        console.error("Falha ao abrir stream do arquivo:", e);
        emitir("physicalcloud:transfer-erro", {
            sessaoId, motivo: "Não foi possível ler o arquivo selecionado.",
        });
        processarFila(sessaoId);
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
        cancelamentoEmitido: false,
    };
    transferencias.set(transferId, entrada);
    ativasEnvio.set(sessaoId, transferId);

    try {
        dc.send(JSON.stringify({
            type: "file-offer", transferId,
            nome: arquivo.name, tamanho: arquivo.size,
            tipo: arquivo.type || "application/octet-stream",
        }));
    } catch (e) {
        console.error("Falha ao enviar file-offer:", e);
        reader.releaseLock();
        limparSeProprio(sessaoId, transferId);
        processarFila(sessaoId);
        return;
    }

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, nome: arquivo.name, tamanho: arquivo.size, direcao: "envio",
    });

    try {
        await aguardarAceitacao(transferId, dc);
    } catch {
        // [Fix.10] rejeitarAceitacaoPendente já foi chamado por quem cancelou
        // (file-cancel, file-error, sessao-encerrada) OU foi timeout. Catch só
        // normaliza entrada e limpa.
        entrada.estado = "cancelado";
        reader.releaseLock();
        limparSeProprio(sessaoId, transferId);
        // @ts-ignore
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    // ── Streaming com backpressure ──────────────────────────────────────────
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
            if (!value) continue;

            let offset = 0;
            while (offset < value.byteLength) {
                if (/** @type {string} */ (entrada.estado) === "cancelado") break;
                if (dc.readyState !== "open") { entrada.estado = "cancelado"; break; }

                const fim = Math.min(offset + TAMANHO_CHUNK, value.byteLength);
                const fatia = value.buffer.slice(value.byteOffset + offset, value.byteOffset + fim);

                // @ts-ignore
                try { dc.send(fatia); }
                catch { entrada.estado = "cancelado"; break; }

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
        try { reader.releaseLock(); } catch { /* noop */ }
        dc.removeEventListener("bufferedamountlow", onBufferLow);
        dc.removeEventListener("close", onClose);
        entrada.cancelarPausa = null;
    }

    if (/** @type {string} */ (entrada.estado) === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        // @ts-ignore
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    let sha256;
    try {
        sha256 = await calcularSha256(await arquivo.arrayBuffer());
    } catch (e) {
        console.error("Falha ao calcular SHA-256:", e);
        entrada.estado = "cancelado";
        try { if (dc.readyState === "open") dc.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* noop */ }
        limparSeProprio(sessaoId, transferId);
        // @ts-ignore
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    if (/** @type {string} */ (entrada.estado) === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        // @ts-ignore
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    try {
        dc.send(JSON.stringify({ type: "file-done", transferId, sha256 }));
        // [Fix.12] arma timeout esperando file-ok / file-error
        armarTimeoutOk(transferId, sessaoId);
    } catch (e) {
        console.error("Falha ao enviar file-done:", e);
        limparSeProprio(sessaoId, transferId);
        // @ts-ignore
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
    }
}

// ─── Fila de envio ─────────────────────────────────────────────────────────

/**
 * @param {string} sessaoId
 */
function processarFila(sessaoId) {
    if (ativasEnvio.has(sessaoId)) return;
    const fila = filas.get(sessaoId);
    if (!fila || fila.length === 0) return;
    const proximo = fila.shift();
    if (proximo) {
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

// ─── Recebimento ────────────────────────────────────────────────────────────

/**
 * @type {Map<string, {
 *   transferId: string, nome: string, tamanho: number, tipo: string,
 *   chunks: BlobPart[], recebidos: number,
 *   writerHandle: FileSystemWritableFileStream | null,
 *   fileHandle: FileSystemFileHandle | null,
 * }>}
 */
const recebimentos = new Map();

/**
 * @param {string} sessaoId
 * @param {{ transferId: any; nome: any; tamanho: any; tipo: any; }} msg
 * @param {RTCDataChannel} dc
 * @param {{ (fn: () => Promise<void>): void; (arg0: () => Promise<void>): void; }} enfileirar
 */
async function tratarFileOffer(sessaoId, msg, dc, enfileirar) {
    const { transferId, nome, tamanho, tipo } = msg;

    if (
        typeof transferId !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(transferId)
    ) return;
    if (typeof nome !== "string" || nome.length === 0 || nome.length > 255) return;
    if (typeof tamanho !== "number" || !Number.isFinite(tamanho) || tamanho < 0) return;

    // [Fix.13] Se já existe entry para esta sessão, aborta writer antigo.
    // Antes, um segundo file-offer (peer bug ou malicioso) orfanava o
    // FileSystemWritableFileStream anterior, mantendo handle aberto até GC.
    const anterior = recebimentos.get(sessaoId);
    if (anterior) {
        if (anterior.writerHandle) anterior.writerHandle.abort().catch(() => {});
        // Rejeita qualquer aceitação pendente do anterior (defensivo)
        rejeitarAceitacaoPendente(anterior.transferId, "Substituído por novo file-offer.");
        recebimentos.delete(sessaoId);
        ativasRecebimento.delete(sessaoId);
        emitir("physicalcloud:transfer-cancelado", {
            transferId: anterior.transferId, sessaoId,
        });
    }

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
            .catch(() => { /* usuário cancelou */ });
    }
}

/**
 * @param {string} sessaoId
 * @param {{ type: any; transferId: string; sha256: any; motivo: string; }} msg
 * @param {{ send: (arg0: string) => void; }} dc
 * @param {{ (fn: any): void; (fn: () => Promise<void>): void; }} enfileirar
 */
async function tratarMensagemJson(sessaoId, msg, dc, enfileirar) {
    if (!msg || typeof msg.type !== "string") return;

    switch (msg.type) {
        case "file-offer":
            // @ts-ignore
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
                            try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." })); } catch { /* noop */ }
                            emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, motivo: "Arquivo corrompido (SHA-256 não confere)." });
                            recebimentos.delete(sessaoId);
                            ativasRecebimento.delete(sessaoId);
                            return;
                        }
                    }
                } catch { /* ignora */ }

                // [Fix.14] Verifica se a sessão ainda está viva APÓS os awaits.
                // Se sessao-encerrada rodou durante o SHA, recebimentos já foi
                // deletado — não emite "concluído" em cima de "cancelado".
                if (!recebimentos.has(sessaoId)) return;

                try { dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId })); } catch { /* noop */ }
                // Deletar ANTES de emitir, para que qualquer sessao-encerrada
                // disparada durante emitir(transfer-concluido) não ache rec.
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
                emitir("physicalcloud:transfer-concluido", {
                    transferId: rec.transferId, sessaoId, nome: rec.nome, tamanho: rec.tamanho, direcao: "recebimento",
                });
            } else {
                const blob = new Blob(rec.chunks, { type: rec.tipo || "application/octet-stream" });
                const buf = await blob.arrayBuffer();
                const sha256Local = await calcularSha256(buf);

                // [Fix.14] idem — checa que recebimentos ainda existe após awaits
                if (!recebimentos.has(sessaoId)) return;

                if (sha256Local !== sha256Remoto) {
                    try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." })); } catch { /* noop */ }
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

                try { dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId })); } catch { /* noop */ }
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
                emitir("physicalcloud:transfer-concluido", {
                    transferId: rec.transferId, sessaoId, nome: rec.nome, tamanho: rec.tamanho, direcao: "recebimento",
                });
            }
            break;
        }

        case "file-cancel": {
            // Recebimento
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === msg.transferId) {
                if (rec.writerHandle) { try { await rec.writerHandle.abort(); } catch { /* noop */ } }
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
            }

            // Envio
            const tidEnvio = ativasEnvio.get(sessaoId);
            if (tidEnvio && tidEnvio === msg.transferId) {
                const e = transferencias.get(tidEnvio);
                if (e) {
                    e.estado = "cancelado";
                    if (typeof e.cancelarPausa === "function") e.cancelarPausa();
                    // @ts-ignore
                    emitirCancelamento(e, sessaoId);
                }
                // [Fix.10] rejeita aceitação pendente — antes a coroutine ficava
                // pendurada 30 s (timeout) após o peer cancelar nesta fase.
                rejeitarAceitacaoPendente(msg.transferId, "Cancelado pelo peer.");
                cancelarTimeoutOk(msg.transferId);
                limparSeProprio(sessaoId, msg.transferId);
                processarFila(sessaoId);
            } else {
                emitir("physicalcloud:transfer-cancelado", { sessaoId, transferId: msg.transferId });
            }
            break;
        }

        case "file-ok": {
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const { nome, tamanho } = entrada;
            cancelarTimeoutOk(tid);
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
            // [Fix.10] mesmo não encontrando entrada, pode haver aguardarAceitacao
            // pendente (peer mandou erro antes de file-accept).
            rejeitarAceitacaoPendente(tid, msg.motivo || "Receptor reportou erro.");
            if (!entrada) break;
            const motivo = msg.motivo || "Receptor reportou erro desconhecido.";
            entrada.estado = "erro";
            cancelarTimeoutOk(tid);
            limparSeProprio(sessaoId, tid);
            emitir("physicalcloud:transfer-erro", { transferId: tid, sessaoId, motivo });
            processarFila(sessaoId);
            break;
        }
    }
}

/**
 * @param {string} sessaoId
 * @param {BlobPart} data
 * @param {{ readyState: string; send: (arg0: string) => void; }} dc
 */
async function tratarChunk(sessaoId, data, dc) {
    const rec = recebimentos.get(sessaoId);
    if (!rec) return;

    if (rec.writerHandle) {
        try {
            await rec.writerHandle.write(data);
        } catch (e) {
            console.error("Erro ao escrever chunk em disco:", e);
            // [Fix.15] Se durante o await outra rota (cancelarTransferencia,
            // sessao-encerrada ou novo file-offer) já removeu/substituiu este
            // rec, não emite erro por cima do cancelado que ela já emitiu.
            if (recebimentos.get(sessaoId) !== rec) return;
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

    const tamanhoChunk = typeof data === "string"
        ? new TextEncoder().encode(data).byteLength
        : data instanceof Blob
            ? data.size
            : data instanceof ArrayBuffer || ArrayBuffer.isView(data)
                ? data.byteLength
                : 0;
    rec.recebidos += tamanhoChunk;
    emitir("physicalcloud:transfer-progresso", {
        transferId: rec.transferId, sessaoId, enviados: rec.recebidos, total: rec.tamanho,
        timestamp: Date.now(),
    });
}

// ─── Integração com WebRTC ──────────────────────────────────────────────────

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
                    try { await tarefa(); } catch (e) { console.error("Erro na fila:", e); }
                }
            }
            ctrl.ocupado = false;
        })();
    }

    /**
     * @param {{ (): Promise<void>; (): Promise<void>; (): Promise<void>; }} fn
     */
    function enfileirar(fn) {
        if (ctrl.cancelado) return;
        ctrl.fila.push(fn);
        processar();
    }

    dc.addEventListener("message", (/** @type {{ data: string; }} */ ev) => {
        if (typeof ev.data === "string") {
            let msg;
            try { msg = JSON.parse(ev.data); } catch { msg = null; }
            if (msg && typeof msg.type === "string" && msg.type.startsWith("file-")) {
                enfileirar(() => tratarMensagemJson(sessaoId, msg, dc, enfileirar));
            }
        // @ts-ignore
        } else if (ev.data instanceof ArrayBuffer) {
            if (ativasRecebimento.has(sessaoId)) {
                enfileirar(() => tratarChunk(sessaoId, ev.data, dc));
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
            // @ts-ignore
            emitirCancelamento(entrada, sessaoId);
        }
        // [Fix.10] rejeita aguardarAceitacao pendente
        rejeitarAceitacaoPendente(tidEnvio, "Sessão encerrada.");
        cancelarTimeoutOk(tidEnvio);
        transferencias.delete(tidEnvio);
        ativasEnvio.delete(sessaoId);
    }

    const tidRec = ativasRecebimento.get(sessaoId);
    if (tidRec) {
        const rec = recebimentos.get(sessaoId);
        if (rec) {
            if (rec.writerHandle) rec.writerHandle.abort().catch(() => {});
            recebimentos.delete(sessaoId);
            emitir("physicalcloud:transfer-cancelado", { transferId: tidRec, sessaoId });
        }
        ativasRecebimento.delete(sessaoId);
    }
});

// ─── Botão "Enviar arquivo" ─────────────────────────────────────────────────

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

    const removerInput = () => { if (input.parentNode) document.body.removeChild(input); };
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

// ─── API pública ────────────────────────────────────────────────────────────

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
                // @ts-ignore
                emitirCancelamento(entrada, sessaoId);
            }
            rejeitarAceitacaoPendente(transferId, "Cancelado pelo usuário.");
            cancelarTimeoutOk(transferId);
            limparSeProprio(sessaoId, transferId);
            processarFila(sessaoId);
        } else {
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === transferId) {
                if (rec.writerHandle) rec.writerHandle.abort().catch(() => {});
                recebimentos.delete(sessaoId);
            }
            ativasRecebimento.delete(sessaoId);
            emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
        }
    },
};