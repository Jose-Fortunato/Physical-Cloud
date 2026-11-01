// @ts-check
// ═══════════════════════════════════════════════════════════════════════════
// PhysicalCloud-transfer.js — FASE 1 (correções críticas)
// #1 #4 #10 #11 #15 + fallback Android para blob
// ═══════════════════════════════════════════════════════════════════════════

const TAMANHO_CHUNK = 256 * 1024;
const BUFFER_ALTO = 4 * 1024 * 1024;
const BUFFER_BAIXO = 512 * 1024;
const TIMEOUT_ACEITE_MS = 120000;
const TIMEOUT_OK_MS = 60000;
const TIMEOUT_OPERACAO_MS = 120000;
const TAMANHO_PAGINA = 100;
const TAMANHO_LOTE = 100;

if (!(/** @type {any} */ (window).PhysicalCloud)) {
    console.error("PhysicalCloud-transfer.js: window.PhysicalCloud não encontrado.");
}

class TransferError extends Error {
    /** @param {string} message @param {string} tipo @param {string} [motivo] */
    constructor(message, tipo, motivo) {
        super(message);
        this.name = "TransferError";
        this.tipo = tipo;
        this.motivo = motivo || message;
    }
}

/** @type {Map<string, RTCDataChannel>} */ const canais = new Map();
/** @type {Map<string, any>} */ const transferencias = new Map();
/** @type {Map<string, Array<{ file: File, operacaoId: string, caminhoRelativo: string }>>} */ const filas = new Map();
/** @type {Map<string, string>} */ const ativasEnvio = new Map();
/** @type {Map<string, string>} */ const ativasRecebimento = new Map();
/** @type {Map<string, { fila: Array<() => Promise<void>>, ocupado: boolean, cancelado: boolean }>} */ const controladoresFila = new Map();

/** @type {Map<string, any>} */ const operacoes = new Map();
/** @type {Map<string, any>} */ const ofertasOperacoes = new Map();
/** @type {Map<string, any>} */ const ofertasPendentes = new Map();
/** @type {Map<string, any>} */ const recebimentos = new Map();
/** @type {Map<string, FileSystemDirectoryHandle>} */ const pastasCompartilhadas = new Map();

const aguardandoAceitacaoMap = new Map();
const aguardandoOperacaoMap = new Map();
const timeoutsOk = new Map();

// ═══ FASE 1 / Bug #10 ═══
/** @type {Map<string, Set<string>>} */
const pullsAguardando = new Map();

/** @param {string} sessaoId @param {string} operacaoId */
function marcarPullAguardando(sessaoId, operacaoId) {
    let set = pullsAguardando.get(sessaoId);
    if (!set) { set = new Set(); pullsAguardando.set(sessaoId, set); }
    set.add(operacaoId);
    // timeout de segurança: limpa após 120s mesmo sem receber operation-start
    setTimeout(() => {
        const s = pullsAguardando.get(sessaoId);
        if (s) {
            s.delete(operacaoId);
            if (s.size === 0) pullsAguardando.delete(sessaoId);
        }
    }, TIMEOUT_OPERACAO_MS);
}

/** @param {string} sessaoId @param {string} operacaoId */
function consumirPullAguardando(sessaoId, operacaoId) {
    const set = pullsAguardando.get(sessaoId);
    if (!set) return false;
    const tinha = set.delete(operacaoId);
    if (set.size === 0) pullsAguardando.delete(sessaoId);
    return tinha;
}

/** @param {string} sessaoId */
function limparPullsAguardando(sessaoId) {
    pullsAguardando.delete(sessaoId);
}

// ─── Utilidades ───

function gerarTransferId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const b = new Uint8Array(16);
    crypto.getRandomValues(b);
    return Array.from(b, x => x.toString(16).padStart(2, "0")).join("");
}

/** @param {BufferSource} buffer */
async function calcularSha256(buffer) {
    const hash = await crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
}

/** @param {string} tipo @param {any} detalhe */
function emitir(tipo, detalhe) {
    window.dispatchEvent(new CustomEvent(tipo, { detail: detalhe }));
}

/** @param {string} sessaoId @param {string | undefined} transferId */
function limparSeProprio(sessaoId, transferId) {
    if (transferId !== undefined && transferencias.has(transferId)) transferencias.delete(transferId);
    if (ativasEnvio.get(sessaoId) === transferId) ativasEnvio.delete(sessaoId);
}

/** @param {any} entrada @param {string} sessaoId */
function emitirCancelamento(entrada, sessaoId) {
    if (entrada.cancelamentoEmitido) return;
    entrada.cancelamentoEmitido = true;
    emitir("physicalcloud:transfer-cancelado", { transferId: entrada.transferId, sessaoId });
}

/** @param {string} sessaoId @returns {boolean} */
function sessaoTemOperacaoAtiva(sessaoId) {
    for (const op of operacoes.values()) {
        if (op.sessaoId === sessaoId) return true;
    }
    return false;
}

// FASE 1: helpers de detecção de ambiente
function temDirectoryPicker() {
    return typeof (/** @type {any} */ (window)).showDirectoryPicker === "function";
}

// ─── Aguardando aceites ───

/** @param {string} transferId @param {RTCDataChannel} dc */
function aguardarAceitacao(transferId, dc) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            aguardandoAceitacaoMap.delete(transferId);
            reject(new TransferError("Tempo esgotado aguardando aceitação.", "timeout"));
        }, TIMEOUT_ACEITE_MS);
        aguardandoAceitacaoMap.set(transferId, {
            resolve: () => { clearTimeout(timeout); resolve(undefined); },
            reject: (/** @type {any} */ err) => { clearTimeout(timeout); reject(err); },
        });
        const onclose = () => {
            const p = aguardandoAceitacaoMap.get(transferId);
            if (p) {
                aguardandoAceitacaoMap.delete(transferId);
                p.reject(new TransferError("Canal fechado.", "channel-closed"));
            }
            dc.removeEventListener("close", onclose);
        };
        dc.addEventListener("close", onclose);
    });
}

/** @param {string} transferId @param {string} motivo @param {string} [tipo] */
function rejeitarAceitacaoPendente(transferId, motivo, tipo = "error") {
    const prom = aguardandoAceitacaoMap.get(transferId);
    if (prom) {
        aguardandoAceitacaoMap.delete(transferId);
        prom.reject(new TransferError(motivo || "Rejeitado", tipo, motivo));
    }
}

/** @param {string} operacaoId @param {RTCDataChannel} dc */
function aguardarDecisaoOperacao(operacaoId, dc) {
    return new Promise((resolve) => {
        const timeout = setTimeout(() => {
            if (aguardandoOperacaoMap.has(operacaoId)) {
                aguardandoOperacaoMap.delete(operacaoId);
                resolve("timeout");
            }
        }, TIMEOUT_OPERACAO_MS);
        aguardandoOperacaoMap.set(operacaoId, {
            resolve: (/** @type {string} */ decisao) => { clearTimeout(timeout); resolve(decisao); },
        });
        const onclose = () => {
            if (aguardandoOperacaoMap.has(operacaoId)) {
                aguardandoOperacaoMap.delete(operacaoId);
                clearTimeout(timeout);
                resolve("closed");
            }
            dc.removeEventListener("close", onclose);
        };
        dc.addEventListener("close", onclose);
    });
}

/** @param {string} operacaoId @param {string} decisao */
function resolverDecisaoOperacao(operacaoId, decisao) {
    const p = aguardandoOperacaoMap.get(operacaoId);
    if (p) { aguardandoOperacaoMap.delete(operacaoId); p.resolve(decisao); }
}

// ─── Timeout de file-ok ───

/** @param {string} transferId @param {string} sessaoId */
function armarTimeoutOk(transferId, sessaoId) {
    const t = setTimeout(() => {
        timeoutsOk.delete(transferId);
        const entrada = transferencias.get(transferId);
        if (!entrada) return;
        entrada.estado = "erro";
        limparSeProprio(sessaoId, transferId);
        emitir("physicalcloud:transfer-erro", {
            transferId, sessaoId, operacaoId: entrada.operacaoId,
            motivo: "Tempo esgotado aguardando confirmação do peer.",
        });
        processarFila(sessaoId);
    }, TIMEOUT_OK_MS);
    timeoutsOk.set(transferId, t);
}

/** @param {string} transferId */
function cancelarTimeoutOk(transferId) {
    const t = timeoutsOk.get(transferId);
    if (t) { clearTimeout(t); timeoutsOk.delete(transferId); }
}

// ─── Explorador remoto ───

/** @param {FileSystemDirectoryHandle} dirHandle @param {string} [caminhoAtual] */
async function listarDiretorio(dirHandle, caminhoAtual = "") {
    const itens = [];
    for await (const entry of dirHandle.values()) {
        const caminho = caminhoAtual ? `${caminhoAtual}/${entry.name}` : entry.name;
        if (entry.kind === "file") {
            const file = await entry.getFile();
            itens.push({ nome: entry.name, caminho, tipo: "file", tamanho: file.size });
        } else {
            itens.push({ nome: entry.name, caminho, tipo: "directory", tamanho: 0 });
        }
    }
    return itens.sort((a, b) => {
        if (a.tipo !== b.tipo) return a.tipo === "directory" ? -1 : 1;
        return a.nome.localeCompare(b.nome);
    });
}

/** @param {string} sessaoId @param {string} caminhoSolicitado @param {number} paginaSolicitada @param {RTCDataChannel} dc */
async function responderListagem(sessaoId, caminhoSolicitado, paginaSolicitada, dc) {
    const raiz = pastasCompartilhadas.get(sessaoId);
    if (!raiz) {
        try {
            dc.send(JSON.stringify({
                type: "dir-list-response", caminho: caminhoSolicitado,
                pagina: 1, totalPaginas: 1, totalItens: 0, itens: [],
                erro: "Nenhuma pasta compartilhada nesta sessão. O outro dispositivo precisa autorizar uma pasta primeiro.",
            }));
        } catch {}
        return;
    }
    try {
        let alvo = raiz;
        if (caminhoSolicitado) {
            const partes = caminhoSolicitado.split("/").filter(Boolean);
            for (const parte of partes) {
                if (parte === ".." || parte === "." || parte.includes("\\")) throw new Error("Caminho inválido");
                alvo = await alvo.getDirectoryHandle(parte);
            }
        }
        const itens = await listarDiretorio(alvo, caminhoSolicitado);
        const totalPaginas = Math.max(1, Math.ceil(itens.length / TAMANHO_PAGINA));
        const pagina = Math.min(Math.max(1, paginaSolicitada || 1), totalPaginas);
        const inicio = (pagina - 1) * TAMANHO_PAGINA;
        dc.send(JSON.stringify({
            type: "dir-list-response", caminho: caminhoSolicitado,
            itens: itens.slice(inicio, inicio + TAMANHO_PAGINA),
            pagina, totalPaginas, totalItens: itens.length,
            temMais: pagina < totalPaginas,
        }));
    } catch (e) {
        console.error("Erro ao listar diretório:", e);
        try {
            dc.send(JSON.stringify({
                type: "dir-list-response", caminho: caminhoSolicitado,
                pagina: 1, totalPaginas: 1, totalItens: 0, itens: [],
                erro: "Não foi possível acessar esta pasta. Verifique se ela ainda existe.",
            }));
        } catch {}
    }
}

/** @param {string} sessaoId @param {string} caminho */
async function obterArquivoPorCaminho(sessaoId, caminho) {
    const raiz = pastasCompartilhadas.get(sessaoId);
    if (!raiz) return null;
    const partes = caminho.split("/").filter(Boolean);
    if (partes.length === 0) return null;
    for (const parte of partes) {
        if (parte === ".." || parte === "." || parte.includes("\\")) return null;
    }
    let dir = raiz;
    for (let i = 0; i < partes.length - 1; i++) dir = await dir.getDirectoryHandle(partes[i]);
    return (await dir.getFileHandle(partes[partes.length - 1])).getFile();
}

/** @param {string} sessaoId @param {string} caminho */
async function obterDiretorioPorCaminho(sessaoId, caminho) {
    const raiz = pastasCompartilhadas.get(sessaoId);
    if (!raiz) return null;
    if (!caminho) return raiz;
    const partes = caminho.split("/").filter(Boolean);
    for (const parte of partes) {
        if (parte === ".." || parte === "." || parte.includes("\\")) return null;
    }
    let dir = raiz;
    for (const parte of partes) dir = await dir.getDirectoryHandle(parte);
    return dir;
}

/** @param {FileSystemDirectoryHandle} dirHandle @param {string} [prefixo] */
async function coletarArquivosRecursivo(dirHandle, prefixo = "") {
    /** @type {Array<{ file: File, caminhoRelativo: string }>} */
    const resultado = [];
    for await (const entry of dirHandle.values()) {
        const caminhoRelativo = prefixo ? `${prefixo}/${entry.name}` : entry.name;
        if (entry.kind === "file") {
            resultado.push({ file: await entry.getFile(), caminhoRelativo });
        } else {
            resultado.push(...await coletarArquivosRecursivo(entry, caminhoRelativo));
        }
    }
    return resultado;
}

// ═══ FIM DA PARTE 1/3 ═══
// ═══ INÍCIO DA PARTE 2/3 ═══

// ─── Envio por operação ───

/**
 * @param {string} sessaoId
 * @param {Array<{ file: File, caminhoRelativo?: string }>} entradas
 * @param {string | null} [operacaoIdForcado]  — usado pelo pull para forçar o ID
 */
async function iniciarOperacaoEnvio(sessaoId, entradas, operacaoIdForcado = null) {
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") {
        emitir("physicalcloud:transfer-erro", { sessaoId, motivo: "Canal não está aberto." });
        return null;
    }
    if (entradas.length === 0) return null;

    if (sessaoTemOperacaoAtiva(sessaoId)) {
        emitir("physicalcloud:transfer-erro", {
            sessaoId,
            motivo: "Já existe uma transferência em andamento para este dispositivo. Aguarde terminar.",
        });
        return null;
    }

    const operacaoId = operacaoIdForcado || gerarTransferId();
    const arquivos = entradas.map(e => ({
        file: e.file,
        caminhoRelativo: e.caminhoRelativo || /** @type {any} */ (e.file).webkitRelativePath || e.file.name,
        nome: e.file.name,
        tamanho: e.file.size,
        estado: "pendente",
    }));
    const totalBytes = arquivos.reduce((s, a) => s + a.tamanho, 0);
    const op = {
        operacaoId, sessaoId, direcao: "envio",
        arquivos, autorizada: false, recusada: false,
        bytesTotais: totalBytes,
        arquivosConcluidos: 0, arquivosComErro: 0,
        loteAtual: 1, totalLotes: Math.ceil(arquivos.length / TAMANHO_LOTE) || 1,
    };
    operacoes.set(operacaoId, op);

    try {
        dc.send(JSON.stringify({
            type: "operation-start", operacaoId,
            totalArquivos: arquivos.length, totalBytes,
        }));
    } catch {
        operacoes.delete(operacaoId);
        emitir("physicalcloud:transfer-erro", { sessaoId, motivo: "Falha ao iniciar operação." });
        return null;
    }

    emitir("physicalcloud:operacao-iniciada", {
        operacaoId, sessaoId, direcao: "envio",
        totalArquivos: arquivos.length, totalBytes,
        totalLotes: op.totalLotes,
    });

    const decisao = await aguardarDecisaoOperacao(operacaoId, dc);
    if (decisao === "accepted") {
        op.autorizada = true;
        let fila = filas.get(sessaoId);
        if (!fila) { fila = []; filas.set(sessaoId, fila); }
        for (const a of arquivos) fila.push({ file: a.file, operacaoId, caminhoRelativo: a.caminhoRelativo });
        processarFila(sessaoId);
        return op;
    }

    op.recusada = decisao === "refused";
    operacoes.delete(operacaoId);
    if (decisao === "refused") {
        emitir("physicalcloud:operacao-recusada", { operacaoId, sessaoId });
    } else {
        emitir("physicalcloud:operacao-cancelada", { operacaoId, sessaoId, direcao: "envio" });
    }
    return null;
}

/** @param {string} sessaoId @param {File[]} arquivos */
function enfileirarArquivos(sessaoId, arquivos) {
    const entradas = arquivos.map(f => ({
        file: f,
        caminhoRelativo: /** @type {any} */ (f).webkitRelativePath || f.name,
    }));
    iniciarOperacaoEnvio(sessaoId, entradas).catch(e => console.error("Erro ao iniciar operação:", e));
}

/**
 * @param {string} sessaoId
 * @param {Array<{ file: File, caminhoRelativo: string }>} entradas
 * @param {string | null} [operacaoIdForcado]
 */
function enfileirarArquivosComCaminho(sessaoId, entradas, operacaoIdForcado = null) {
    iniciarOperacaoEnvio(sessaoId, entradas, operacaoIdForcado)
        .catch(e => console.error("Erro ao iniciar operação (pull):", e));
}

/** @param {string} sessaoId */
function processarFila(sessaoId) {
    if (ativasEnvio.has(sessaoId)) return;
    const fila = filas.get(sessaoId);
    if (!fila || fila.length === 0) return;
    const proximo = fila.shift();
    if (proximo) enviarArquivo(sessaoId, proximo).catch(e => console.error("Erro em enviarArquivo:", e));
}

/**
 * @param {string} sessaoId
 * @param {{ file: File, operacaoId: string, caminhoRelativo: string }} item
 */
async function enviarArquivo(sessaoId, item) {
    const dc = canais.get(sessaoId);
    const op = operacoes.get(item.operacaoId);
    if (!dc || dc.readyState !== "open" || !op || !op.autorizada || op.recusada) {
        processarFila(sessaoId);
        return;
    }

    let reader = null;
    try { reader = item.file.stream().getReader(); }
    catch {
        emitir("physicalcloud:transfer-erro", { sessaoId, nome: item.file.name, motivo: "Não foi possível ler o arquivo selecionado." });
        op.arquivosComErro++;
        processarFila(sessaoId);
        return;
    }

    const transferId = gerarTransferId();
    const entrada = {
        transferId, sessaoId, operacaoId: op.operacaoId,
        nome: item.file.name, tamanho: item.file.size,
        tipo: item.file.type || "application/octet-stream",
        caminhoRelativo: item.caminhoRelativo,
        enviados: 0, estado: "enviando",
        cancelarPausa: /** @type {(() => void) | null} */ (null),
        cancelamentoEmitido: false,
    };
    transferencias.set(transferId, entrada);
    ativasEnvio.set(sessaoId, transferId);

    try {
        dc.send(JSON.stringify({
            type: "file-offer", transferId, operacaoId: op.operacaoId,
            nome: item.file.name, caminhoRelativo: item.caminhoRelativo,
            tamanho: item.file.size, tipo: entrada.tipo,
        }));
    } catch {
        try { reader.releaseLock(); } catch {}
        limparSeProprio(sessaoId, transferId);
        op.arquivosComErro++;
        processarFila(sessaoId);
        return;
    }

    emitir("physicalcloud:transfer-inicio", {
        transferId, sessaoId, operacaoId: op.operacaoId,
        nome: item.file.name, tamanho: item.file.size,
        caminhoRelativo: item.caminhoRelativo, direcao: "envio",
    });

    try { await aguardarAceitacao(transferId, dc); }
    catch (/** @type {any} */ err) {
        const tipo = err?.tipo || "error";
        const motivo = err?.motivo || err?.message || "Erro desconhecido";
        try { reader.releaseLock(); } catch {}
        limparSeProprio(sessaoId, transferId);
        if (tipo === "cancel") {
            entrada.estado = "cancelado";
            emitirCancelamento(entrada, sessaoId);
        } else if (tipo === "refused") {
            entrada.estado = "recusado";
            op.arquivosComErro++;
            emitir("physicalcloud:transfer-recusado", { transferId, sessaoId, motivo });
        } else {
            entrada.estado = "erro";
            op.arquivosComErro++;
            emitir("physicalcloud:transfer-erro", { transferId, sessaoId, operacaoId: op.operacaoId, motivo });
        }
        processarFila(sessaoId);
        return;
    }

    dc.bufferedAmountLowThreshold = BUFFER_BAIXO;
    let pausado = false;
    /** @type {((value?: any) => void) | null} */ let resolverPausa = null;
    const liberarPausa = () => {
        if (pausado && resolverPausa) { const r = resolverPausa; pausado = false; resolverPausa = null; r(); }
    };
    const onBufferLow = () => liberarPausa();
    const onClose = () => liberarPausa();
    dc.addEventListener("bufferedamountlow", onBufferLow);
    dc.addEventListener("close", onClose);
    entrada.cancelarPausa = liberarPausa;

    try {
        while (true) {
            if (entrada.estado === "cancelado") break;
            const { done, value } = await reader.read();
            if (done) break;
            if (!value) continue;
            let offset = 0;
            while (offset < value.byteLength) {
                if (entrada.estado === "cancelado") break;
                if (dc.readyState !== "open") { entrada.estado = "cancelado"; break; }
                const fim = Math.min(offset + TAMANHO_CHUNK, value.byteLength);
                const fatia = new Uint8Array(fim - offset);
                fatia.set(new Uint8Array(value.buffer, value.byteOffset + offset, fim - offset));
                try { dc.send(fatia); } catch { entrada.estado = "cancelado"; break; }
                entrada.enviados += fatia.byteLength;
                emitir("physicalcloud:transfer-progresso", {
                    transferId, sessaoId, operacaoId: op.operacaoId,
                    enviados: entrada.enviados, total: item.file.size, timestamp: Date.now(),
                });
                if (dc.bufferedAmount > BUFFER_ALTO) {
                    pausado = true;
                    await new Promise((res) => { resolverPausa = res; });
                }
                offset = fim;
            }
        }
    } finally {
        try { reader.releaseLock(); } catch {}
        dc.removeEventListener("bufferedamountlow", onBufferLow);
        dc.removeEventListener("close", onClose);
        entrada.cancelarPausa = null;
    }

    if (entrada.estado === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    let sha256;
    try { sha256 = await calcularSha256(await item.file.arrayBuffer()); }
    catch {
        entrada.estado = "erro";
        try { if (dc.readyState === "open") dc.send(JSON.stringify({ type: "file-cancel", transferId })); } catch {}
        limparSeProprio(sessaoId, transferId);
        op.arquivosComErro++;
        emitir("physicalcloud:transfer-erro", { transferId, sessaoId, operacaoId: op.operacaoId, motivo: "Falha ao calcular SHA-256." });
        processarFila(sessaoId);
        return;
    }

    if (entrada.estado === "cancelado") {
        limparSeProprio(sessaoId, transferId);
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
        return;
    }

    try {
        dc.send(JSON.stringify({ type: "file-done", transferId, sha256 }));
        armarTimeoutOk(transferId, sessaoId);
    } catch {
        limparSeProprio(sessaoId, transferId);
        emitirCancelamento(entrada, sessaoId);
        processarFila(sessaoId);
    }
}

// ─── Recebimento ───

/** @param {string} sessaoId @param {any} msg @param {RTCDataChannel} dc */
function tratarOperationStart(sessaoId, msg, dc) {
    const operacaoId = typeof msg.operacaoId === "string" ? msg.operacaoId : null;
    if (!operacaoId) return;
    if (ofertasOperacoes.has(operacaoId)) return;

    const sessoes = /** @type {any} */ (window).PhysicalCloud?.sessoes;
    const sessaoInfo = sessoes ? sessoes.get(sessaoId) : null;
    const totalArquivos = Number(msg.totalArquivos) || 0;
    const totalBytes = Number(msg.totalBytes) || 0;
    const op = {
        operacaoId, sessaoId,
        nomeRemetente: sessaoInfo ? sessaoInfo.nome : "Outro dispositivo",
        totalArquivos, totalBytes,
        autorizada: false, recusada: false,
        destinoHandle: /** @type {FileSystemDirectoryHandle | null} */ (null),
        modoSalvamento: "blob",
        arquivosComErro: 0,
        dc,
    };
    ofertasOperacoes.set(operacaoId, op);

    // FASE 1 / Bug #10: auto-aceite de pull (por operacaoId esperado)
    if (consumirPullAguardando(sessaoId, operacaoId)) {
        (/** @type {any} */ (window).PhysicalCloudTransfer).aceitarOperacaoAutomatica(operacaoId).catch((/** @type {any} */ e) => {
            console.error("Falha ao auto-aceitar pull:", e);
        });
        return;
    }

    emitir("physicalcloud:operacao-recebida", {
        operacaoId, sessaoId, nomeRemetente: op.nomeRemetente,
        totalArquivos, totalBytes,
        totalLotes: Math.ceil(totalArquivos / TAMANHO_LOTE) || 1,
    });
}

/** @param {string} sessaoId @param {any} msg @param {RTCDataChannel} dc */
function tratarFileOffer(sessaoId, msg, dc) {
    const { transferId, operacaoId, nome, caminhoRelativo, tamanho, tipo } = msg;
    if (typeof transferId !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(transferId)) return;
    if (typeof nome !== "string" || nome.length === 0 || nome.length > 255) return;
    if (typeof tamanho !== "number" || !Number.isFinite(tamanho) || tamanho < 0) return;
    const tipoSeguro = (typeof tipo === "string" && /^[\w.+-]+\/[\w.+-]+$/.test(tipo)) ? tipo : "application/octet-stream";

    if (!operacaoId || !ofertasOperacoes.has(operacaoId)) {
        if (!operacaoId) return;
        tratarOperationStart(sessaoId, { operacaoId, totalArquivos: 1, totalBytes: tamanho }, dc);
    }
    const op = ofertasOperacoes.get(operacaoId);
    if (!op) return;
    if (op.recusada) {
        try { dc.send(JSON.stringify({ type: "file-error", transferId, motivo: "Operação recusada pelo destinatário." })); } catch {}
        return;
    }
    if (!op.autorizada) {
        ofertasPendentes.set(transferId, {
            transferId, operacaoId, sessaoId, nome,
            caminhoRelativo: typeof caminhoRelativo === "string" ? caminhoRelativo : nome,
            tamanho, tipo: tipoSeguro, dc,
        });
        return;
    }
    aceitarArquivoInterno({
        transferId, operacaoId, sessaoId, nome,
        caminhoRelativo: typeof caminhoRelativo === "string" ? caminhoRelativo : nome,
        tamanho, tipo: tipoSeguro, dc,
    });
}

// ═══ FIM DA PARTE 2/3 ═══

// ═══ INÍCIO DA PARTE 3/3 ═══

/**
 * @param {{ transferId: string, operacaoId: string, sessaoId: string, nome: string, caminhoRelativo: string, tamanho: number, tipo: string, dc: RTCDataChannel }} oferta
 */
async function aceitarArquivoInterno(oferta) {
    const op = ofertasOperacoes.get(oferta.operacaoId);
    if (!op || op.recusada) {
        try { oferta.dc.send(JSON.stringify({ type: "file-error", transferId: oferta.transferId, motivo: "Operação recusada." })); } catch {}
        return;
    }

    const rec = {
        transferId: oferta.transferId, operacaoId: oferta.operacaoId, sessaoId: oferta.sessaoId,
        nome: oferta.nome, caminhoRelativo: oferta.caminhoRelativo,
        tamanho: oferta.tamanho, tipo: oferta.tipo,
        chunks: /** @type {ArrayBuffer[]} */ ([]), recebidos: 0,
        writerHandle: /** @type {FileSystemWritableFileStream | null} */ (null),
        fileHandle: /** @type {FileSystemFileHandle | null} */ (null),
        modo: "blob",
    };

    if (op.modoSalvamento === "diretorio" && op.destinoHandle) {
        try {
            const partes = oferta.caminhoRelativo.split("/").filter(Boolean);
            let dir = op.destinoHandle;
            for (let i = 0; i < partes.length - 1; i++) {
                if (partes[i] === ".." || partes[i] === "." || partes[i].includes("\\")) throw new Error("Caminho inválido");
                dir = await dir.getDirectoryHandle(partes[i], { create: true });
            }
            const nomeFinal = partes[partes.length - 1] || oferta.nome;
            const fh = await dir.getFileHandle(nomeFinal, { create: true });
            rec.writerHandle = await fh.createWritable();
            rec.fileHandle = fh;
            rec.modo = "diretorio";
        } catch (e) {
            console.error("Falha ao preparar arquivo de destino, caindo para blob:", e);
            rec.writerHandle = null; rec.fileHandle = null; rec.modo = "blob";
        }
    }

    const anterior = recebimentos.get(oferta.sessaoId);
    if (anterior && anterior.transferId !== oferta.transferId && anterior.writerHandle) {
        try { await anterior.writerHandle.abort(); } catch {}
    }
    recebimentos.set(oferta.sessaoId, rec);
    ativasRecebimento.set(oferta.sessaoId, oferta.transferId);

    try {
        oferta.dc.send(JSON.stringify({ type: "file-accept", transferId: oferta.transferId }));
    } catch {
        recebimentos.delete(oferta.sessaoId);
        ativasRecebimento.delete(oferta.sessaoId);
        return;
    }

    emitir("physicalcloud:transfer-inicio", {
        transferId: oferta.transferId, sessaoId: oferta.sessaoId, operacaoId: oferta.operacaoId,
        nome: oferta.nome, tamanho: oferta.tamanho,
        caminhoRelativo: oferta.caminhoRelativo, direcao: "recebimento",
    });
}

/** @param {string} sessaoId @param {ArrayBuffer} data @param {RTCDataChannel} dc */
async function tratarChunk(sessaoId, data, dc) {
    const rec = recebimentos.get(sessaoId);
    if (!rec) return;
    if (rec.writerHandle) {
        try { await rec.writerHandle.write(data); }
        catch {
            if (recebimentos.get(sessaoId) !== rec) return;
            try { if (dc.readyState === "open") dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Erro ao escrever no disco." })); } catch {}
            try { await rec.writerHandle.abort(); } catch {}
            recebimentos.delete(sessaoId);
            ativasRecebimento.delete(sessaoId);
            emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, operacaoId: rec.operacaoId, motivo: "Erro ao escrever no disco." });
            return;
        }
    } else {
        rec.chunks.push(data);
    }
    rec.recebidos += data.byteLength;
    emitir("physicalcloud:transfer-progresso", {
        transferId: rec.transferId, sessaoId, operacaoId: rec.operacaoId,
        enviados: rec.recebidos, total: rec.tamanho, timestamp: Date.now(),
    });
}

/** @param {string} sessaoId @param {any} msg @param {RTCDataChannel} dc */
async function tratarFileDone(sessaoId, msg, dc) {
    const rec = recebimentos.get(sessaoId);
    if (!rec || rec.transferId !== msg.transferId) return;
    const sha256Remoto = msg.sha256;

    if (rec.recebidos !== rec.tamanho) {
        try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Tamanho do arquivo não confere." })); } catch {}
        if (rec.writerHandle) { try { await rec.writerHandle.abort(); } catch {} }
        recebimentos.delete(sessaoId);
        ativasRecebimento.delete(sessaoId);
        emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, operacaoId: rec.operacaoId, motivo: "Arquivo incompleto." });
        return;
    }

    if (rec.writerHandle) {
        try { await rec.writerHandle.close(); }
        catch {
            try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha ao salvar arquivo." })); } catch {}
            recebimentos.delete(sessaoId);
            ativasRecebimento.delete(sessaoId);
            emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, operacaoId: rec.operacaoId, motivo: "Falha ao salvar arquivo." });
            return;
        }
        if (rec.fileHandle) {
            try {
                const f = await rec.fileHandle.getFile();
                const buf = await f.arrayBuffer();
                if (await calcularSha256(buf) !== sha256Remoto) {
                    try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." })); } catch {}
                    recebimentos.delete(sessaoId);
                    ativasRecebimento.delete(sessaoId);
                    emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, operacaoId: rec.operacaoId, motivo: "Arquivo corrompido." });
                    return;
                }
            } catch { /* handle sem releitura */ }
        }
        try { dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId })); } catch {}
        recebimentos.delete(sessaoId);
        ativasRecebimento.delete(sessaoId);
        emitir("physicalcloud:transfer-concluido", {
            transferId: rec.transferId, sessaoId, operacaoId: rec.operacaoId,
            nome: rec.nome, tamanho: rec.tamanho, caminhoRelativo: rec.caminhoRelativo,
            direcao: "recebimento",
        });
        return;
    }

    const blob = new Blob(rec.chunks, { type: rec.tipo || "application/octet-stream" });
    const buf = await blob.arrayBuffer();
    if (await calcularSha256(buf) !== sha256Remoto) {
        try { dc.send(JSON.stringify({ type: "file-error", transferId: rec.transferId, motivo: "Falha de integridade SHA-256." })); } catch {}
        recebimentos.delete(sessaoId);
        ativasRecebimento.delete(sessaoId);
        emitir("physicalcloud:transfer-erro", { sessaoId, transferId: rec.transferId, operacaoId: rec.operacaoId, motivo: "Arquivo corrompido." });
        return;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = rec.nome;
    document.body.appendChild(a); a.click();
    setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 5000);
    try { dc.send(JSON.stringify({ type: "file-ok", transferId: rec.transferId })); } catch {}
    recebimentos.delete(sessaoId);
    ativasRecebimento.delete(sessaoId);
    emitir("physicalcloud:transfer-concluido", {
        transferId: rec.transferId, sessaoId, operacaoId: rec.operacaoId,
        nome: rec.nome, tamanho: rec.tamanho, caminhoRelativo: rec.caminhoRelativo,
        direcao: "recebimento",
    });
}

// ─── Roteador de mensagens ───

/** @param {string} sessaoId @param {any} msg @param {RTCDataChannel} dc */
async function tratarMensagemJson(sessaoId, msg, dc) {
    if (!msg || typeof msg.type !== "string") return;
    switch (msg.type) {

        case "operation-start":
            tratarOperationStart(sessaoId, msg, dc);
            break;

        case "operation-accept":
            resolverDecisaoOperacao(msg.operacaoId, "accepted");
            break;

        case "operation-refused":
            resolverDecisaoOperacao(msg.operacaoId, "refused");
            break;

        case "operation-cancel": {
            const op = operacoes.get(msg.operacaoId);
            if (op) {
                op.recusada = true;
                const sessaoLocal = op.sessaoId;
                const fila = filas.get(sessaoLocal);
                if (fila) {
                    for (let i = fila.length - 1; i >= 0; i--) {
                        if (fila[i].operacaoId === msg.operacaoId) fila.splice(i, 1);
                    }
                }
                const ativa = ativasEnvio.get(sessaoLocal);
                if (ativa) {
                    const e = transferencias.get(ativa);
                    if (e && e.operacaoId === msg.operacaoId) {
                        e.estado = "cancelado";
                        if (e.cancelarPausa) e.cancelarPausa();
                        try { dc.send(JSON.stringify({ type: "file-cancel", transferId: ativa })); } catch {}
                        limparSeProprio(sessaoLocal, ativa);
                        emitirCancelamento(e, sessaoLocal);
                    }
                }
                operacoes.delete(msg.operacaoId);
                emitir("physicalcloud:operacao-cancelada", { operacaoId: msg.operacaoId, sessaoId: sessaoLocal, direcao: "envio" });
                processarFila(sessaoLocal);
            }
            break;
        }

        case "file-offer":
            tratarFileOffer(sessaoId, msg, dc);
            break;

        case "file-accept": {
            const prom = aguardandoAceitacaoMap.get(msg.transferId);
            if (prom) { aguardandoAceitacaoMap.delete(msg.transferId); prom.resolve(undefined); }
            break
                    }

        case "file-done":
            await tratarFileDone(sessaoId, msg, dc);
            break;

        case "file-cancel": {
            const rec = recebimentos.get(sessaoId);
            if (rec && rec.transferId === msg.transferId) {
                if (rec.writerHandle) { try { await rec.writerHandle.abort(); } catch {} }
                recebimentos.delete(sessaoId);
                ativasRecebimento.delete(sessaoId);
                emitir("physicalcloud:transfer-cancelado", { transferId: rec.transferId, sessaoId });
            }
            const tidEnvio = ativasEnvio.get(sessaoId);
            if (tidEnvio && tidEnvio === msg.transferId) {
                const e = transferencias.get(tidEnvio);
                if (e) {
                    e.estado = "cancelado";
                    if (e.cancelarPausa) e.cancelarPausa();
                    emitirCancelamento(e, sessaoId);
                }
                rejeitarAceitacaoPendente(msg.transferId, "Cancelado pelo peer.", "cancel");
                cancelarTimeoutOk(msg.transferId);
                limparSeProprio(sessaoId, msg.transferId);
                processarFila(sessaoId);
            }
            break;
        }

        case "file-ok": {
            const tid = msg.transferId;
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const op = operacoes.get(entrada.operacaoId);
            const { nome, tamanho, caminhoRelativo, operacaoId } = entrada;
            cancelarTimeoutOk(tid);
            limparSeProprio(sessaoId, tid);
            if (op) {
                op.arquivosConcluidos++;
                emitir("physicalcloud:operacao-progresso", {
                    operacaoId: op.operacaoId, sessaoId,
                    arquivosConcluidos: op.arquivosConcluidos,
                    arquivosComErro: op.arquivosComErro,
                    totalArquivos: op.arquivos.length,
                    loteAtual: Math.min(Math.ceil((op.arquivosConcluidos + op.arquivosComErro) / TAMANHO_LOTE) || 1, op.totalLotes),
                    totalLotes: op.totalLotes,
                });
                if (op.arquivosConcluidos + op.arquivosComErro >= op.arquivos.length) {
                    operacoes.delete(op.operacaoId);
                    if (op.arquivosComErro === 0) {
                        emitir("physicalcloud:operacao-concluida", {
                            operacaoId: op.operacaoId, sessaoId, direcao: "envio",
                            arquivosConcluidos: op.arquivosConcluidos,
                            arquivosComErro: 0, totalArquivos: op.arquivos.length,
                        });
                    } else {
                        emitir("physicalcloud:operacao-concluida-com-erros", {
                            operacaoId: op.operacaoId, sessaoId, direcao: "envio",
                            arquivosConcluidos: op.arquivosConcluidos,
                            arquivosComErro: op.arquivosComErro, totalArquivos: op.arquivos.length,
                        });
                    }
                }
            }
            emitir("physicalcloud:transfer-concluido", {
                transferId: tid, sessaoId, nome, tamanho, caminhoRelativo, operacaoId, direcao: "envio",
            });
            processarFila(sessaoId);
            break;
        }

        case "file-error": {
            const tid = msg.transferId;
            const motivo = msg.motivo || "Receptor reportou erro desconhecido.";
            const tipo = /recus/i.test(motivo) ? "refused" : "error";
            rejeitarAceitacaoPendente(tid, motivo, tipo);
            const entrada = transferencias.get(tid);
            if (!entrada) break;
            const op = operacoes.get(entrada.operacaoId);
            entrada.estado = "erro";
            cancelarTimeoutOk(tid);
            limparSeProprio(sessaoId, tid);
            if (op) op.arquivosComErro++;
            emitir("physicalcloud:transfer-erro", { transferId: tid, sessaoId, operacaoId: entrada.operacaoId, motivo });
            processarFila(sessaoId);
            break;
        }

        case "dir-list-request": {
            const caminho = typeof msg.caminho === "string" ? msg.caminho : "";
            const pagina = Number(msg.pagina) || 1;
            await responderListagem(sessaoId, caminho, pagina, dc);
            break;
        }

        case "dir-list-response": {
            if (msg.erro) {
                emitir("physicalcloud:lista-recebida", {
                    erro: msg.erro, caminho: msg.caminho || "",
                    pagina: msg.pagina, totalPaginas: msg.totalPaginas, totalItens: msg.totalItens,
                });
            } else {
                emitir("physicalcloud:lista-recebida", {
                    itens: msg.itens || [], caminho: msg.caminho || "",
                    pagina: msg.pagina, totalPaginas: msg.totalPaginas,
                    totalItens: msg.totalItens, temMais: !!msg.temMais,
                });
            }
            break;
        }

        case "share-folder-request":
            emitir("physicalcloud:share-folder-solicitado", { sessaoId });
            break;

        case "share-folder-response":
            emitir("physicalcloud:share-folder-resultado", {
                sessaoId, sucesso: !!msg.sucesso, erro: msg.erro || null,
            });
            break;

        case "file-pull-request": {
            const caminho = typeof msg.caminho === "string" ? msg.caminho : "";
            const tipoCaminho = msg.tipoCaminho === "directory" ? "directory" : "file";
            // FASE 1 / Bug #10: usa o transferId do pedido como operacaoId forçado.
            // O receptor já registrou esse ID em pullsAguardando — assim o auto-aceite
            // só dispara para o pull específico, nunca para uma operação alheia.
            const operacaoIdForcado = typeof msg.transferId === "string" ? msg.transferId : null;
            try {
                if (tipoCaminho === "file") {
                    const arquivo = await obterArquivoPorCaminho(sessaoId, caminho);
                    if (!arquivo) {
                        try { dc.send(JSON.stringify({ type: "file-error", transferId: operacaoIdForcado || "pull", motivo: "Arquivo não encontrado." })); } catch {}
                        return;
                    }
                    enfileirarArquivosComCaminho(sessaoId, [{ file: arquivo, caminhoRelativo: caminho }], operacaoIdForcado);
                } else {
                    const dir = await obterDiretorioPorCaminho(sessaoId, caminho);
                    if (!dir) {
                        try { dc.send(JSON.stringify({ type: "file-error", transferId: operacaoIdForcado || "pull", motivo: "Pasta não encontrada." })); } catch {}
                        return;
                    }
                    const lista = await coletarArquivosRecursivo(dir, caminho);
                    if (lista.length === 0) {
                        try { dc.send(JSON.stringify({ type: "file-error", transferId: operacaoIdForcado || "pull", motivo: "Pasta vazia." })); } catch {}
                        return;
                    }
                    enfileirarArquivosComCaminho(sessaoId, lista, operacaoIdForcado);
                }
            } catch (e) {
                console.error("Erro em file-pull-request:", e);
                try { dc.send(JSON.stringify({ type: "file-error", transferId: operacaoIdForcado || "pull", motivo: "Falha ao ler no dispositivo remetente." })); } catch {}
            }
            break;
        }
    }
}

// ─── Ciclo de vida do canal ───

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
                if (tarefa) { try { await tarefa(); } catch (e) { console.error("Erro na fila:", e); } }
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

    dc.addEventListener("message", (/** @type {MessageEvent} */ ev) => {
        if (typeof ev.data === "string") {
            let msg;
            try { msg = JSON.parse(ev.data); } catch { msg = null; }
            if (msg && typeof msg.type === "string") {
                if (msg.type.startsWith("file-") || msg.type.startsWith("dir-") ||
                    msg.type.startsWith("operation-") || msg.type.startsWith("share-")) {
                    enfileirar(() => tratarMensagemJson(sessaoId, msg, dc));
                }
            }
        } else if (ev.data instanceof ArrayBuffer) {
            if (ativasRecebimento.has(sessaoId)) enfileirar(() => tratarChunk(sessaoId, ev.data, dc));
        }
    });

    window.dispatchEvent(new CustomEvent("physicalcloud:sessao-pronta-para-ui", { detail: { sessaoId } }));
});

window.addEventListener("physicalcloud:sessao-encerrada", (event) => {
    const { sessaoId } = /** @type {CustomEvent} */ (event).detail;
    canais.delete(sessaoId);
    filas.delete(sessaoId);
    pastasCompartilhadas.delete(sessaoId);
    limparPullsAguardando(sessaoId);

    const ctrl = controladoresFila.get(sessaoId);
    if (ctrl) { ctrl.cancelado = true; ctrl.fila.length = 0; controladoresFila.delete(sessaoId); }

    for (const [tid, oferta] of Array.from(ofertasPendentes.entries())) {
        if (oferta.sessaoId === sessaoId) ofertasPendentes.delete(tid);
    }

    // FASE 1 / Bug #15: limpa operações órfãs emitindo operacao-cancelada
    for (const [opId, op] of Array.from(operacoes.entries())) {
        if (op.sessaoId === sessaoId) {
            operacoes.delete(opId);
            emitir("physicalcloud:operacao-cancelada", { operacaoId: opId, sessaoId, direcao: "envio" });
        }
    }
    for (const [opId, op] of Array.from(ofertasOperacoes.entries())) {
        if (op.sessaoId === sessaoId) {
            ofertasOperacoes.delete(opId);
            emitir("physicalcloud:operacao-cancelada", { operacaoId: opId, sessaoId, direcao: "recebimento" });
        }
    }

    const tidEnvio = ativasEnvio.get(sessaoId);
    if (tidEnvio) {
        const entrada = transferencias.get(tidEnvio);
        if (entrada) {
            entrada.estado = "cancelado";
            if (entrada.cancelarPausa) entrada.cancelarPausa();
            emitirCancelamento(entrada, sessaoId);
        }
        rejeitarAceitacaoPendente(tidEnvio, "Sessão encerrada.", "channel-closed");
        cancelarTimeoutOk(tidEnvio);
        transferencias.delete(tidEnvio);
        ativasEnvio.delete(sessaoId);
    }

    const tidRec = ativasRecebimento.get(sessaoId);
    if (tidRec) {
        const rec = recebimentos.get(sessaoId);
        if (rec && rec.writerHandle) rec.writerHandle.abort().catch(() => {});
        recebimentos.delete(sessaoId);
        ativasRecebimento.delete(sessaoId);
        emitir("physicalcloud:transfer-cancelado", { transferId: tidRec, sessaoId });
    }
});

// ─── Input file global (compatibilidade com evento legado) ───

window.addEventListener("physicalcloud:solicitar-envio", () => {
    if (canais.size === 0) { window.dispatchEvent(new CustomEvent("physicalcloud:transfer-sem-canal")); return; }
    const input = document.createElement("input");
    input.type = "file"; input.multiple = true; input.style.display = "none";
    document.body.appendChild(input);
    const removerInput = () => { if (input.parentNode) document.body.removeChild(input); };
    input.addEventListener("cancel", removerInput);
    input.addEventListener("change", () => {
        const arquivos = Array.from(input.files || []);
        removerInput();
        if (arquivos.length === 0) return;
        const sessaoIds = Array.from(canais.keys());
        if (sessaoIds.length === 1) enfileirarArquivos(sessaoIds[0], arquivos);
        else window.dispatchEvent(new CustomEvent("physicalcloud:escolher-destino", { detail: { arquivos, sessaoIds } }));
    });
    input.click();
});

// ─── Compartilhamento de pasta remoto ───

/** @param {string} sessaoId */
function solicitarCompartilhamentoRemoto(sessaoId) {
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") return false;
    try { dc.send(JSON.stringify({ type: "share-folder-request" })); return true; }
    catch { return false; }
}

/** @param {string} sessaoId @param {boolean} sucesso @param {string} [erro] */
function notificarPastaCompartilhada(sessaoId, sucesso, erro) {
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") return;
    try { dc.send(JSON.stringify({ type: "share-folder-response", sucesso: !!sucesso, erro: erro || null })); } catch {}
}

window.addEventListener("physicalcloud:solicitar-exploracao", (event) => {
    const { sessaoId, caminho, pagina } = /** @type {CustomEvent} */ (event).detail || {};
    if (!sessaoId) return;
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") return;
    try {
        dc.send(JSON.stringify({ type: "dir-list-request", caminho: caminho || "", pagina: pagina || 1 }));
    } catch {}
});

// FASE 1 / Bug #10: o requestor envia o operacaoId esperado no transferId.
// O emissor (file-pull-request) usa esse mesmo ID como operacaoId forçado.
// Assim o auto-aceite é exato — nunca pega operação alheia.
window.addEventListener("physicalcloud:solicitar-download-remoto", (event) => {
    const { sessaoId, caminho, tipoCaminho } = /** @type {CustomEvent} */ (event).detail || {};
    if (!sessaoId || !caminho) return;
    const dc = canais.get(sessaoId);
    if (!dc || dc.readyState !== "open") return;
    const operacaoId = gerarTransferId();
    marcarPullAguardando(sessaoId, operacaoId);
    try {
        dc.send(JSON.stringify({
            type: "file-pull-request", caminho,
            tipoCaminho: tipoCaminho || "file",
            transferId: operacaoId,
        }));
    } catch {
        const set = pullsAguardando.get(sessaoId);
        if (set) { set.delete(operacaoId); if (set.size === 0) pullsAguardando.delete(sessaoId); }
    }
});

window.addEventListener("physicalcloud:share-folder-solicitado", async (event) => {
    const { sessaoId } = /** @type {CustomEvent} */ (event).detail;
    if (!canais.has(sessaoId)) return;
    if (!temDirectoryPicker()) {
        notificarPastaCompartilhada(sessaoId, false, "Este dispositivo não suporta compartilhar pasta.");
        /** @type {any} */ (window).PhysicalCloud?.aviso?.("Este dispositivo não suporta compartilhar pasta.");
        return;
    }
    try {
        const handle = await /** @type {any} */ (window).showDirectoryPicker({ mode: "read" });
        pastasCompartilhadas.set(sessaoId, handle);
        notificarPastaCompartilhada(sessaoId, true);
        /** @type {any} */ (window).PhysicalCloud?.aviso?.("Pasta compartilhada com o outro dispositivo.");
    } catch (e) {
        const erro = /** @type {{ name?: string, message?: string }} */ (e);
        const msg = erro && erro.name === "AbortError" ? "Seleção cancelada." : (erro?.message || "Falha ao selecionar pasta.");
        notificarPastaCompartilhada(sessaoId, false, msg);
    }
});

// ─── API pública ───

(/** @type {any} */ (window)).PhysicalCloudTransfer = {
    enfileirarArquivos, canais, transferencias, operacoes, ofertasOperacoes,
    solicitarCompartilhamentoRemoto,

    /**
     * FASE 1 / Bug #1:
     *  - AbortError → recusa legítima (usuário cancelou o picker)
     *  - Qualquer outro erro (SecurityError/NotAllowedError/API ausente) → cai para
     *    modo blob e prossegue com a transferência. NUNCA marca como recusada.
     * @param {string} operacaoId
     */
    async aceitarOperacao(operacaoId) {
        const op = ofertasOperacoes.get(operacaoId);
        if (!op || op.autorizada || op.recusada) return;
        if (!op.dc || op.dc.readyState !== "open") {
            ofertasOperacoes.delete(operacaoId);
            return;
        }

        let usouBlob = false;

        if (temDirectoryPicker()) {
            try {
                op.destinoHandle = await /** @type {any} */ (window).showDirectoryPicker({ mode: "readwrite" });
                op.modoSalvamento = "diretorio";
            } catch (/** @type {any} */ e) {
                const tipoErro = (e && e.name) || "";
                if (tipoErro === "AbortError") {
                    // Recusa legítima: usuário fechou o picker de propósito.
                    op.recusada = true;
                    try { op.dc.send(JSON.stringify({ type: "operation-refused", operacaoId })); } catch {}
                    resolverDecisaoOperacao(operacaoId, "refused");
                    ofertasOperacoes.delete(operacaoId);
                    for (const [tid, o] of ofertasPendentes) {
                        if (o.operacaoId === operacaoId) ofertasPendentes.delete(tid);
                    }
                    return;
                }
                // Falha de API: NUNCA recusa. Cai para blob e segue.
                console.warn("showDirectoryPicker falhou, caindo para blob:", e);
                op.modoSalvamento = "blob";
                usouBlob = true;
            }
        } else {
            op.modoSalvamento = "blob";
            usouBlob = true;
        }

        op.autorizada = true;
        try { op.dc.send(JSON.stringify({ type: "operation-accept", operacaoId })); } catch {}

        emitir("physicalcloud:operacao-iniciada", {
            operacaoId, sessaoId: op.sessaoId, direcao: "recebimento",
            totalArquivos: op.totalArquivos, totalBytes: op.totalBytes,
            totalLotes: Math.ceil(op.totalArquivos / TAMANHO_LOTE) || 1,
        });

        if (usouBlob) {
            emitir("physicalcloud:modo-salvamento-blob", {
                operacaoId, sessaoId: op.sessaoId,
                totalArquivos: op.totalArquivos,
            });
        }

        const pendentes = Array.from(ofertasPendentes.values()).filter(o => o.operacaoId === operacaoId);
        for (const o of pendentes) {
            ofertasPendentes.delete(o.transferId);
            await aceitarArquivoInterno(o);
        }
    },

    /**
     * FASE 1 / Bug #4: auto-aceite de pull.
     * Nunca tenta picker, sempre blob. Só é chamado quando o operacaoId
     * bate com um pull registrado por esta sessão (Bug #10).
     * @param {string} operacaoId
     */
    async aceitarOperacaoAutomatica(operacaoId) {
        const op = ofertasOperacoes.get(operacaoId);
        if (!op || op.autorizada || op.recusada) return;
        if (!op.dc || op.dc.readyState !== "open") {
            ofertasOperacoes.delete(operacaoId);
            return;
        }

        op.modoSalvamento = "blob";
        op.autorizada = true;
        try { op.dc.send(JSON.stringify({ type: "operation-accept", operacaoId })); } catch {}

        emitir("physicalcloud:operacao-iniciada", {
            operacaoId, sessaoId: op.sessaoId, direcao: "recebimento",
            totalArquivos: op.totalArquivos, totalBytes: op.totalBytes,
            totalLotes: Math.ceil(op.totalArquivos / TAMANHO_LOTE) || 1,
        });

        emitir("physicalcloud:modo-salvamento-blob", {
            operacaoId, sessaoId: op.sessaoId, totalArquivos: op.totalArquivos,
        });

        const pendentes = Array.from(ofertasPendentes.values()).filter(o => o.operacaoId === operacaoId);
        for (const o of pendentes) {
            ofertasPendentes.delete(o.transferId);
            await aceitarArquivoInterno(o);
        }
    },

    /** @param {string} operacaoId */
    recusarOperacao(operacaoId) {
        const op = ofertasOperacoes.get(operacaoId);
        if (!op) return;
        op.recusada = true; op.autorizada = false;
        try { op.dc.send(JSON.stringify({ type: "operation-refused", operacaoId })); } catch {}
        resolverDecisaoOperacao(operacaoId, "refused");
        for (const [tid, o] of ofertasPendentes) {
            if (o.operacaoId === operacaoId) {
                ofertasPendentes.delete(tid);
                try { o.dc.send(JSON.stringify({ type: "file-error", transferId: tid, motivo: "Recusado pelo usuário." })); } catch {}
            }
        }
        ofertasOperacoes.delete(operacaoId);
    },

    /**
     * FASE 1 / Bug #11: cancela operação no emissor ou receptor.
     * No receptor, aborta writers em voo e notifica o emissor.
     * @param {string} operacaoId
     */
    cancelarOperacao(operacaoId) {
        const opEnvio = operacoes.get(operacaoId);
        if (opEnvio) {
            opEnvio.recusada = true;
            const sessaoId = opEnvio.sessaoId;
            const dc = canais.get(sessaoId);

            if (dc && dc.readyState === "open") {
                try { dc.send(JSON.stringify({ type: "operation-cancel", operacaoId })); } catch {}
            }
            const fila = filas.get(sessaoId);
            if (fila) {
                for (let i = fila.length - 1; i >= 0; i--) {
                    if (fila[i].operacaoId === operacaoId) fila.splice(i, 1);
                }
            }
            const ativa = ativasEnvio.get(sessaoId);
            if (ativa) {
                const e = transferencias.get(ativa);
                if (e && e.operacaoId === operacaoId) {
                    e.estado = "cancelado";
                    if (e.cancelarPausa) e.cancelarPausa();
                    emitirCancelamento(e, sessaoId);
                    if (dc && dc.readyState === "open") {
                        try { dc.send(JSON.stringify({ type: "file-cancel", transferId: ativa })); } catch {}
                    }
                    limparSeProprio(sessaoId, ativa);
                }
            }
            operacoes.delete(operacaoId);
            emitir("physicalcloud:operacao-cancelada", { operacaoId, sessaoId, direcao: "envio" });
            processarFila(sessaoId);
            return;
        }

        const opRec = ofertasOperacoes.get(operacaoId);
        if (opRec) {
            opRec.recusada = true;
            try { opRec.dc.send(JSON.stringify({ type: "operation-cancel", operacaoId })); } catch {}

            for (const [sid, rec] of Array.from(recebimentos.entries())) {
                if (rec.operacaoId === operacaoId) {
                    if (rec.writerHandle) rec.writerHandle.abort().catch(() => {});
                    recebimentos.delete(sid);
                    ativasRecebimento.delete(sid);
                    emitir("physicalcloud:transfer-cancelado", { transferId: rec.transferId, sessaoId: sid });
                }
            }

            for (const [tid, o] of Array.from(ofertasPendentes.entries())) {
                if (o.operacaoId === operacaoId) {
                    ofertasPendentes.delete(tid);
                    try { o.dc.send(JSON.stringify({ type: "file-error", transferId: tid, motivo: "Cancelado pelo receptor." })); } catch {}
                }
            }

            emitir("physicalcloud:operacao-cancelada", { operacaoId, sessaoId: opRec.sessaoId, direcao: "recebimento" });
            ofertasOperacoes.delete(operacaoId);
        }
    },

    /** @param {any} transferId */
    cancelarTransferencia(transferId) {
        if (!transferId) return;
        let sessaoId = null, sentido = null;
        for (const [sid, tid] of ativasEnvio) { if (tid === transferId) { sessaoId = sid; sentido = "envio"; break; } }
        if (!sessaoId) { for (const [sid, tid] of ativasRecebimento) { if (tid === transferId) { sessaoId = sid; sentido = "recebimento"; break; } } }
        if (!sessaoId || !sentido) return;
        const dc = canais.get(sessaoId);
        if (dc && dc.readyState === "open") { try { dc.send(JSON.stringify({ type: "file-cancel", transferId })); } catch {} }
        if (sentido === "envio") {
            const entrada = transferencias.get(transferId);
            if (entrada) {
                entrada.estado = "cancelado";
                if (entrada.cancelarPausa) entrada.cancelarPausa();
                emitirCancelamento(entrada, sessaoId);
            }
            rejeitarAceitacaoPendente(transferId, "Cancelado pelo usuário.", "cancel");
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

// ═══ FIM DA PARTE 3/3 ═══