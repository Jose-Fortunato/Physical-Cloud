// @ts-check

const STUN_SERVERS = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun.cloudflare.com:3478" },
];

const NOME_CANAL = "physicalcloud";

/**
 * @typedef {{ pc: RTCPeerConnection, dc: RTCDataChannel | null, estado: string, sessaoId: string }} EntradaPeer
 */

/** @type {Map<string, EntradaPeer>} */
const peers = new Map();

const api = /** @type {any} */ (window).PhysicalCloud;

if (!api) {
    console.error("PhysicalCloud-webrtc.js: window.PhysicalCloud não encontrado. Carregue PhysicalCloud.js primeiro.");
}

/**
 * @param {string} sessaoId
 * @param {{ tipo: string; candidate?: RTCIceCandidateInit; sdp?: string | undefined; }} dados
 */
function enviarSinal(sessaoId, dados) {
    api.enviar({ type: "sinal", sessaoId, dados });
}

/**
 * @param {string} sessaoId
 * @param {string} estado
 */
function atualizarEstadoPeer(sessaoId, estado) {
    const entrada = peers.get(sessaoId);
    if (entrada) entrada.estado = estado;
    window.dispatchEvent(new CustomEvent("physicalcloud:peer-estado", { detail: { sessaoId, estado } }));
}

/** @param {string} sessaoId */
function fecharPeer(sessaoId) {
    const entrada = peers.get(sessaoId);
    if (!entrada) return;
    try { entrada.dc?.close(); } catch { /* ignorar */ }
    try { entrada.pc.close(); } catch { /* ignorar */ }
    peers.delete(sessaoId);
}

/**
 * @param {RTCPeerConnection} pc
 * @param {string} sessaoId
 */
function configurarPc(pc, sessaoId) {
    pc.onicecandidate = (event) => {
        if (event.candidate) {
            enviarSinal(sessaoId, { tipo: "ice", candidate: event.candidate.toJSON() });
        }
    };

    pc.onconnectionstatechange = () => {
        const estado = pc.connectionState;
        atualizarEstadoPeer(sessaoId, estado);
        if (estado === "failed" || estado === "closed") {
            fecharPeer(sessaoId);
        }
    };
}

/**
 * @param {RTCDataChannel} dc
 * @param {string} sessaoId
 */
function configurarDc(dc, sessaoId) {
    const entrada = peers.get(sessaoId);
    if (entrada) entrada.dc = dc;

    dc.binaryType = "arraybuffer";

    let handshakeRecebido = false;
    let handshakeEnviado = false;

    function verificar() {
        if (handshakeRecebido && handshakeEnviado) {
            atualizarEstadoPeer(sessaoId, "verificado");
            window.dispatchEvent(new CustomEvent("physicalcloud:canal-aberto", { detail: { sessaoId, dc } }));
        }
    }

    dc.onopen = () => {
        atualizarEstadoPeer(sessaoId, "canal-aberto");
        try {
            dc.send(JSON.stringify({ type: "pc-ok" }));
            handshakeEnviado = true;
        } catch (e) {
            console.error("Falha ao enviar handshake:", e);
        }
        verificar();
    };

    dc.onmessage = (event) => {
        if (typeof event.data === "string") {
            let msg;
            try { msg = JSON.parse(event.data); } catch { msg = null; }
            if (msg && msg.type === "pc-ok") {
                handshakeRecebido = true;
                verificar();
                return;
            }
            window.dispatchEvent(new CustomEvent("physicalcloud:mensagem", { detail: { sessaoId, dados: msg ?? event.data } }));
        } else {
            window.dispatchEvent(new CustomEvent("physicalcloud:mensagem", { detail: { sessaoId, dados: event.data } }));
        }
    };

    dc.onclose = () => {
        atualizarEstadoPeer(sessaoId, "canal-fechado");
    };

    dc.onerror = (event) => {
        console.error(`DataChannel erro (sessão ${sessaoId}):`, event);
        atualizarEstadoPeer(sessaoId, "canal-erro");
    };
}

/**
 * Esta ponta é o CLIENTE — cria RTCPeerConnection, DataChannel, gera e envia offer.
 * @param {string} sessaoId
 */
async function iniciarComoCliente(sessaoId) {
    if (peers.has(sessaoId)) return;

    const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
    /** @type {EntradaPeer} */
    const entrada = { pc, dc: null, estado: "iniciando", sessaoId };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    const dc = pc.createDataChannel(NOME_CANAL, { ordered: true });
    configurarDc(dc, sessaoId);
    entrada.dc = dc;

    try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        enviarSinal(sessaoId, { tipo: "offer", sdp: pc.localDescription?.sdp });
        atualizarEstadoPeer(sessaoId, "offer-enviado");
    } catch (e) {
        console.error("Falha ao criar offer:", e);
        fecharPeer(sessaoId);
    }
}

/**
 * Esta ponta é o SERVIDOR — cria RTCPeerConnection e aguarda offer via sinal.
 * @param {string} sessaoId
 */
function prepararComoServidor(sessaoId) {
    if (peers.has(sessaoId)) return;

    const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
    /** @type {EntradaPeer} */
    const entrada = { pc, dc: null, estado: "aguardando-offer", sessaoId };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    pc.ondatachannel = (event) => {
        configurarDc(event.channel, sessaoId);
        entrada.dc = event.channel;
    };
}

/**
 * Processa uma mensagem de sinalização recebida via WebSocket e repassa ao RTCPeerConnection.
 * @param {string} sessaoId
 * @param {{ tipo: string, sdp?: string, candidate?: RTCIceCandidateInit }} dados
 */
async function processarSinal(sessaoId, dados) {
    const entrada = peers.get(sessaoId);
    if (!entrada) return;

    const { pc } = entrada;

    if (dados.tipo === "offer") {
        if (!dados.sdp) return;
        try {
            await pc.setRemoteDescription({ type: "offer", sdp: dados.sdp });
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            enviarSinal(sessaoId, { tipo: "answer", sdp: pc.localDescription?.sdp });
            atualizarEstadoPeer(sessaoId, "answer-enviado");
        } catch (e) {
            console.error("Falha ao processar offer:", e);
            fecharPeer(sessaoId);
        }
        return;
    }

    if (dados.tipo === "answer") {
        if (!dados.sdp) return;
        try {
            await pc.setRemoteDescription({ type: "answer", sdp: dados.sdp });
            atualizarEstadoPeer(sessaoId, "conectando");
        } catch (e) {
            console.error("Falha ao processar answer:", e);
            fecharPeer(sessaoId);
        }
        return;
    }

    if (dados.tipo === "ice") {
        if (!dados.candidate) return;
        try {
            await pc.addIceCandidate(new RTCIceCandidate(dados.candidate));
        } catch (e) {
            console.error("Falha ao adicionar ICE candidate:", e);
        }
        return;
    }
}

// ─── Escuta eventos emitidos pelo PhysicalCloud.js ────────────────────────────

window.addEventListener("physicalcloud:sinal", (event) => {
    const { sessaoId, dados } = /** @type {CustomEvent} */ (event).detail;
    if (!sessaoId || !dados || typeof dados.tipo !== "string") return;
    processarSinal(String(sessaoId), dados);
});

window.addEventListener("physicalcloud:sessao-ativa", (event) => {
    const { sessaoId, papel } = /** @type {CustomEvent} */ (event).detail;
    if (papel === "cliente") iniciarComoCliente(String(sessaoId));
    if (papel === "servidor") prepararComoServidor(String(sessaoId));
});

window.addEventListener("physicalcloud:sessao-encerrada", (event) => {
    const { sessaoId } = /** @type {CustomEvent} */ (event).detail;
    fecharPeer(String(sessaoId));
});

// ─── API pública ──────────────────────────────────────────────────────────────

api.peers = peers;
api.fecharPeer = fecharPeer;
