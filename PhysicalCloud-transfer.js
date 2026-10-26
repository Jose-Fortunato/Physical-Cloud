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
//
// Limitações desta versão:
//   - Uma transferência por sessão por vez (fila se houver mais de uma).
//   - File System Access API usada para salvar sem carregar em RAM.
//   - Em navegadores sem File System Access (Android Chrome < 86), fallback para Blob + download link.
//   - Receptor aceita automaticamente (confirmação de UI planejada para Bloco C).

const TAMANHO_CHUNK = 256 * 1024; // 256 KB
const BUFFER_ALTO   = 4 * 1024 * 1024; // 4 MB — pausa envio
const BUFFER_BAIXO  = 512 * 1024;      // 512 KB — retoma envio

const api = /** @type {any} */ (window).PhysicalCloud;

if (!api) {
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
 * }} EntradaTransfer
 */

/** sessaoId → DataChannel aberto */
/** @type {Map<string, RTCDataChannel>} */
const canais = new Map();

/** transferId → estado da transferência */
/** @type {Map<string, EntradaTransfer>} */
const transferencias = new Map();

/** sessaoId → fila de arquivos pendentes para enviar */
/** @type {Map<string, File[]>} */
const filas = new Map();

/** sessaoId → transferId ativa (envio) */
/** @type {Map<string, string>} */
const ativasEnvio = new Map();

/** sessaoId → transferId ativa (recebimento) */
/** @type {Map<string, string>} */
const ativasRecebimento = new Map();

// ─── SHA-256 via Web Crypto ───────────────────────────────────────────────────

/**
 * @param {ArrayBuffer} buffer
 * @returns {Promise<string>}
 */
async function calcularSha256(buffer) {
    const hash = await crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
}

// ─── Gerador de ID de transferência ──────────────────────────────────────────

function gerarTransferId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const b = new Uint8Array(16);
    crypto.getRandomValues(b);
    return Array.from(b, x => x.toString(16).padStart(2, "0")).join("");
}

// ─── Eventos para a UI ───────────────────────────────────────────────────────

/**
 * @param {string} tipo
 * @param {object} detalhe
 */
function emitir(tipo, detalhe) {
    window.dispatchEvent(new CustomEvent(tipo, { detail: detalhe }));
}

// ─── Envio ────────────────────────────────────────────────────────────────────

/**
 * Envia um arquivo via DataChannel para uma sessão.
 *
 * Streaming via ReadableStream com backpressure (bufferedAmountLowThreshold).
 * SHA-256 calculado com arquivo.arrayBuffer() APÓS o streaming terminar.
 *
 * Nota sobre tee() e RAM (4ª auditoria):
 *   tee() parecia uma boa ideia para calcular SHA em paralelo, mas a spec exige
 *   que o runtime armazene internamente todo chunk já lido por um branch e ainda
 *   não pelo outro. Com backpressure no envio (DataChannel ordered + janela de
 *   congestionamento), o branch do SHA vai muito à frente e acumula o arquivo
 *   inteiro em RAM — exatamente o problema que tentávamos evitar.
 *
 *   Solução correta: streaming puro para envio; SHA com arrayBuffer() após o
 *   streaming terminar. O pico de RAM (~tamanho do arquivo) acontece uma vez,
 *   apenas após todo o envio estar completo, nunca ao mesmo tempo.
 *
 * Backpressure via addEventListener("bufferedamountlow").
 *
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
        transferId,
        sessaoId,
        nome: arquivo.name,
        tamanho: arquivo.size,
        tipo: arquivo.type || "application/octet-stream",
        enviados: 0,
        estado: "enviando",
    };
    transferencias.set(transferId, entrada);
    ativasEnvio.set(sessaoId, transferId);

    // Envia metadados
    dc.send(JSON.stringify({
        type: "file-offer",
        transferId,
        nome: arquivo.name,
        tamanho: arquivo.size,
        tipo: arquivo.type || "application/octet-stream",
    }));

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, nome: arquivo.name, tamanho: arquivo.size, direcao: "envio",
    });

    // Aguarda aceitação do receptor
    try {
        await aguardarAceitacao(transferId, dc);
    } catch (e) {
        entrada.estado = "cancelado";
        transferencias.delete(transferId);
        ativasEnvio.delete(sessaoId);
        emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
        processarFila(sessaoId);
        return;
    }

    // ── Streaming via ReadableStream com backpressure ─────────────────────────
    const reader = arquivo.stream().getReader();

    dc.bufferedAmountLowThreshold = BUFFER_BAIXO;
    let pausado = false;
    /** @type {((value?: void | PromiseLike<void>) => void) | null} */
    let resolverPausa = null;
    const onBufferLow = () => {
        if (pausado && resolverPausa) {
            pausado = false;
            resolverPausa();
            resolverPausa = null;
        }
    };
    dc.addEventListener("bufferedamountlow", onBufferLow);

    try {
        while (true) {
            if (entrada.estado === "cancelado") break;

            const { done, value } = await reader.read();
            if (done) break;

            let offset = 0;
            while (offset < value.byteLength) {
                // @ts-ignore
                if (entrada.estado === "cancelado") break;

                const fim = Math.min(offset + TAMANHO_CHUNK, value.byteLength);
                const fatia = value.buffer.slice(value.byteOffset + offset, value.byteOffset + fim);

                dc.send(fatia);
                entrada.enviados += fatia.byteLength;

                emitir("physicalcloud:transfer-progresso", {
                    transferId, sessaoId, enviados: entrada.enviados, total: arquivo.size,
                });

                if (dc.bufferedAmount > BUFFER_ALTO) {
                    pausado = true;
                    await new Promise(res => { resolverPausa = res; });
                }

                offset = fim;
            }
        }
    } finally {
        reader.releaseLock();
        dc.removeEventListener("bufferedamountlow", onBufferLow);
    }

    if (entrada.estado === "cancelado") {
        transferencias.delete(transferId);
        ativasEnvio.delete(sessaoId);
        processarFila(sessaoId);
        return;
    }

    // ── SHA-256 calculado APÓS o streaming — pico de RAM pontual e isolado ────
    //
    // arquivo.arrayBuffer() lê o arquivo do disco uma vez, após todo o envio
    // terminar. O pico de RAM (~tamanho do arquivo) é breve e nunca se sobrepõe
    // ao buffer do DataChannel, que já foi drenado neste ponto.
    let sha256;
    try {
        sha256 = await calcularSha256(await arquivo.arrayBuffer());
    } catch (e) {
        // BUG CORRIGIDO (4ª auditoria): sha256 = "" enviava "file-done" com SHA inválido,
        // fazendo o receptor reportar "arquivo corrompido" mesmo que o arquivo estivesse
        // íntegro. Correto é cancelar a transferência.
        console.error("Falha ao calcular SHA-256:", e);
        entrada.estado = "cancelado";
        if (dc.readyState === "open") {
            dc.send(JSON.stringify({ type: "file-cancel", transferId }));
        }
        transferencias.delete(transferId);
        ativasEnvio.delete(sessaoId);
        emitir("physicalcloud:transfer-cancelado", { transferId, sessaoId });
        processarFila(sessaoId);
        return;
    }

    // Envia sinal de conclusão com SHA-256
    dc.send(JSON.stringify({ type: "file-done", transferId, sha256 }));
    // Não emite transfer-concluido aqui — aguarda file-ok/file-error do receptor.
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
    if (proximo) enviarArquivo(sessaoId, proximo);
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
 *   transferId: string,
 *   nome: string,
 *   tamanho: number,
 *   tipo: string,
 *   chunks: ArrayBuffer[],
 *   recebidos: number,
 *   writerHandle: FileSystemWritableFileStream | null,
 *   fileHandle: FileSystemFileHandle | null,
 * }>}
 */
const recebimentos = new Map();

/**
 * BUG CRÍTICO CORRIGIDO (3ª auditoria): picker bloqueava com await ANTES de
 * recebimentos.set(), então chunks que chegavam enquanto o usuário escolhia
 * o destino eram descartados silenciosamente em tratarChunk (rec === undefined).
 *
 * Fluxo correto (picker não-bloqueante):
 *   1. Registra recebimentos.set() IMEDIATAMENTE com writerHandle = null
 *   2. Registra ativasRecebimento IMEDIATAMENTE
 *   3. Envia file-accept — remetente começa a enviar; chunks vão para rec.chunks (RAM)
 *   4. Abre picker em paralelo com .then() — sem bloquear o recebimento de chunks
 *   5. Quando writer fica pronto: flush dos chunks acumulados em RAM para o writer
 *
 * Se o picker for cancelado ou não disponível, o fallback Blob/RAM já está ativo
 * porque writerHandle ficou null — tratarChunk acumula em rec.chunks normalmente.
 *
 * @param {string} sessaoId
 * @param {any} msg
 * @param {RTCDataChannel} dc
 * @param {(fn: () => Promise<void>) => void} enfileirar função de enfileiramento da sessão
 */
async function tratarFileOffer(sessaoId, msg, dc, enfileirar) {
    const { transferId, nome, tamanho, tipo } = msg;

    // BUG CORRIGIDO (5ª auditoria): validar transferId e tamanho vindos do remoto.
    // transferId sem validação aceitaria "__proto__", "constructor" ou strings enormes.
    // tamanho sem validação aceitaria Infinity ou negativos, causando Uint8Array inválido.
    if (
        typeof transferId !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(transferId)
    ) return;
    if (!nome || typeof nome !== "string") return;
    if (typeof tamanho !== "number" || !Number.isFinite(tamanho) || tamanho < 0) return;

    // 1. Registra imediatamente com writerHandle provisoriamente null.
    //    Chunks que chegarem antes do picker estar pronto vão para rec.chunks (RAM).
    recebimentos.set(sessaoId, {
        transferId, nome, tamanho, tipo,
        chunks: [],
        recebidos: 0,
        writerHandle: null,
        fileHandle: null,
    });
    ativasRecebimento.set(sessaoId, transferId);

    // 2. Aceita agora — remetente começa a enviar. Chunks já são capturados.
    // TODO (Bloco C): emitir physicalcloud:confirmar-recebimento e aguardar UI.
    dc.send(JSON.stringify({ type: "file-accept", transferId }));

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, nome, tamanho, direcao: "recebimento",
    });

    // 3. Abre o picker em paralelo — não bloqueia o recebimento de chunks.
    //    O flush é enfileirado na MESMA fila da sessão para não correr em paralelo
    //    com chunks que já estão na fila aguardando escrita no writer.
    const temFSAPI = "showSaveFilePicker" in window;
    if (temFSAPI) {
        // @ts-ignore
        window.showSaveFilePicker({
            suggestedName: nome,
            types: [{ description: "Arquivo", accept: { [tipo || "application/octet-stream"]: [] } }],
        })
            .then(/** @param {FileSystemFileHandle} fh */ (fh) => {
                // BUG CORRIGIDO (5ª auditoria): flush encadeado na mesma fila dos chunks.
                // Antes, o flush rodava fora da fila — um chunk na fila podia correr em
                // paralelo com o for() do flush no mesmo FileSystemWritableFileStream.
                enfileirar(async () => {
                    const wh = await fh.createWritable();
                    const rec = recebimentos.get(sessaoId);
                    if (!rec || rec.transferId !== transferId) {
                        // Transferência já terminou antes do flush — aborta writer
                        await wh.abort().catch(() => {});
                        return;
                    }
                    // Flush dos chunks acumulados em RAM enquanto o picker estava aberto
                    for (const chunk of rec.chunks) {
                        await wh.write(chunk);
                    }
                    rec.chunks = []; // libera RAM
                    rec.writerHandle = wh;
                    rec.fileHandle = fh;
                });
            })
            .catch(() => {
                // Usuário cancelou o picker ou API falhou — fallback Blob em RAM já está ativo
                // (rec.writerHandle continua null; tratarChunk continua acumulando em rec.chunks)
            });
    }
    // Se não tem FSA, rec.writerHandle permanece null → fallback Blob automático em tratarChunk
}

/**
 * @param {string} sessaoId
 * @param {any} msg
 * @param {RTCDataChannel} dc
 * @param {(fn: () => Promise<void>) => void} enfileirar função de enfileiramento da sessão
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
                    dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha ao salvar arquivo." }));
                    recebimentos.delete(sessaoId);
                    ativasRecebimento.delete(sessaoId);
                    return;
                }

                // Verifica integridade relendo o arquivo do disco
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
                } catch { /* ignora erro de verificação — arquivo já está salvo */ }

                dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId }));
                emitir("physicalcloud:transfer-concluido", {
                    transferId: rec.transferId, sessaoId, nome: rec.nome, tamanho: rec.tamanho, direcao: "recebimento",
                });

            } else {
                // Fallback Blob
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
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === msg.transferId) {
                if (rec.writerHandle) {
                    try { await rec.writerHandle.abort(); } catch { /* ignorar */ }
                }
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
            }
            const tidEnvio = ativasEnvio.get(sessaoId);
            if (tidEnvio && tidEnvio === msg.transferId) {
                const e = transferencias.get(tidEnvio);
                if (e) e.estado = "cancelado";
            }
            emitir("physicalcloud:transfer-cancelado", { sessaoId, transferId: msg.transferId });
            break;
        }

        case "file-ok": {
            // BUG 10 CORRIGIDO (auditoria anterior): receptor confirmou SHA-256 com sucesso.
            // BUG 2 (esta auditoria): usa sessaoId do parâmetro da função, não entrada.sessaoId,
            // que é mais confiável e direto.
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const { nome, tamanho } = entrada;
            transferencias.delete(tid);
            ativasEnvio.delete(sessaoId);
            emitir("physicalcloud:transfer-concluido", {
                transferId: tid, sessaoId, nome, tamanho, direcao: "envio",
            });
            processarFila(sessaoId);
            break;
        }

        case "file-error": {
            // Receptor reportou falha (SHA inválido, disco cheio, etc.)
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const motivo = msg.motivo || "Receptor reportou erro desconhecido.";
            entrada.estado = "erro";
            transferencias.delete(tid);
            ativasEnvio.delete(sessaoId);
            emitir("physicalcloud:transfer-erro", { transferId: tid, sessaoId, motivo });
            processarFila(sessaoId);
            break;
        }
    }
}

/**
 * @param {string} sessaoId
 * @param {ArrayBuffer} data
 */
async function tratarChunk(sessaoId, data) {
    const rec = recebimentos.get(sessaoId);
    if (!rec) return;

    if (rec.writerHandle) {
        try {
            await rec.writerHandle.write(data);
        } catch (e) {
            console.error("Erro ao escrever chunk em disco:", e);
        }
    } else {
        rec.chunks.push(data);
    }

    rec.recebidos += data.byteLength;
    emitir("physicalcloud:transfer-progresso", {
        transferId: rec.transferId, sessaoId, enviados: rec.recebidos, total: rec.tamanho,
    });
}

// ─── Integração com eventos do WebRTC ────────────────────────────────────────

window.addEventListener("physicalcloud:canal-aberto", (event) => {
    const { sessaoId, dc } = /** @type {CustomEvent} */ (event).detail;
    canais.set(sessaoId, dc);

    // BUG CORRIGIDO (4ª auditoria): serialização necessária para evitar writes concorrentes
    // no mesmo FileSystemWritableFileStream, cujo contrato não garante ordem em paralelo.
    //
    // BUG CORRIGIDO (5ª auditoria): padrão cadeia = cadeia.then() acumulava ~4096 Promises
    // encadeadas para um arquivo de 1 GB (chunks de 256 KB). Cada .then() mantém referência
    // à Promise anterior impedindo o GC — memória cresce linearmente com nº de chunks.
    //
    // Solução: fila + mutex. Cada Promise resolve e é coletada pelo GC imediatamente.
    // ocupado: boolean que garante no máximo uma tarefa ativa por vez.
    // fila: array de funções () => Promise<void> aguardando execução.
    // processar(): drena a fila sequencialmente sem acumular referências.
    let ocupado = false;
    /** @type {Array<() => Promise<void>>} */
    const fila = [];

    function processar() {
        if (ocupado) return;
        ocupado = true;
        (async () => {
            while (fila.length > 0) {
                const tarefa = fila.shift();
                if (tarefa) {
                    try { await tarefa(); } catch (e) { console.error("Erro na fila de transferência:", e); }
                }
            }
            ocupado = false;
        })();
    }

    /** @param {() => Promise<void>} fn */
    function enfileirar(fn) {
        fila.push(fn);
        processar();
    }

    dc.addEventListener("message", (/** @type {{ data: string | ArrayBuffer; }} */ ev) => {
        if (typeof ev.data === "string") {
            let msg;
            try { msg = JSON.parse(ev.data); } catch { msg = null; }

            if (msg && typeof msg.type === "string" && msg.type.startsWith("file-")) {
                // Enfileira para não interfolhar com writes pendentes de chunks anteriores.
                // tratarMensagemJson recebe enfileirar para repassar a tratarFileOffer,
                // que o usa para encadear o flush do picker na mesma fila.
                enfileirar(() => tratarMensagemJson(sessaoId, msg, dc, enfileirar));
            }
            // Mensagens não-file-* (ex.: pc-ok) tratadas pelo handler do webrtc.js
        } else if (ev.data instanceof ArrayBuffer) {
            if (ativasRecebimento.has(sessaoId)) {
                enfileirar(() => tratarChunk(sessaoId, /** @type {ArrayBuffer} */ (ev.data)));
            }
        }
    });
});

window.addEventListener("physicalcloud:sessao-encerrada", (event) => {
    const { sessaoId } = /** @type {CustomEvent} */ (event).detail;
    canais.delete(sessaoId);
    filas.delete(sessaoId);

    const tidEnvio = ativasEnvio.get(sessaoId);
    if (tidEnvio) {
        const entrada = transferencias.get(tidEnvio);
        if (entrada) entrada.estado = "cancelado";
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
     * @param {string} sessaoId
     */
    cancelarTransferencia(sessaoId) {
        const tid = ativasEnvio.get(sessaoId) || ativasRecebimento.get(sessaoId);
        if (!tid) return;
        const entrada = transferencias.get(tid);
        if (entrada) entrada.estado = "cancelado";
        const dc = canais.get(sessaoId);
        if (dc && dc.readyState === "open") {
            dc.send(JSON.stringify({ type: "file-cancel", transferId: tid }));
        }
    },
};