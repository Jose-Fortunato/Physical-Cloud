// @ts-check

const STUN_SERVERS = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun.cloudflare.com:3478" },
];

const NOME_CANAL = "physicalcloud";

/**
 * @typedef {{ pc: RTCPeerConnection, dc: RTCDataChannel | null, estado: string, sessaoId: string, candidatosPendentes: RTCIceCandidateInit[], descricaoRemotaDefinida: boolean, everConnected: boolean }} EntradaPeer
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
    // Remove de peers ANTES de chamar pc.close() para que qualquer disparo
    // síncrono de onconnectionstatechange causado pelo close() encontre
    // peers.has() === false e não execute o bloco de cleanup novamente.
    peers.delete(sessaoId);
    try { entrada.dc?.close(); } catch { /* ignorar */ }
    try { entrada.pc.close(); } catch { /* ignorar */ }
    // [Fix.4/Item5] Emite "fechado" explicitamente — dc.onclose e
    // onconnectionstatechange podem não disparar (canal já null ou close falhou).
    atualizarEstadoPeer(sessaoId, "fechado");
}

/**
 * @param {EntradaPeer} entrada
 */
async function processarCandidatosPendentes(entrada) {
    const { pc, candidatosPendentes } = entrada;
    while (candidatosPendentes.length > 0) {
        const c = candidatosPendentes.shift();
        if (!c) continue;
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
        } catch (e) {
            console.warn("Falha ao adicionar ICE candidate pendente:", e);
        }
    }
}

/**
 * [Fix.4/C5] Consulta getStats() para confirmar tipo de conexão (host/srflx/relay).
 * Retorna o candidateType do par nomeado vencedor, ou "desconhecido"/"erro".
 * @param {RTCPeerConnection} pc
 * @returns {Promise<string>}
 */
async function verificarConexaoDireta(pc) {
    try {
        const stats = await pc.getStats();
        /** @type {any} */
        let parNomeado = null;
        stats.forEach(/** @param {any} report */ (report) => {
            if (report.type === "candidate-pair" && report.nominated && report.state === "succeeded") {
                parNomeado = report;
            }
        });
        if (!parNomeado) return "desconhecido";
        let tipoLocal = "desconhecido";
        stats.forEach(/** @param {any} report */ (report) => {
            if (report.type === "local-candidate" && report.id === parNomeado.localCandidateId) {
                tipoLocal = report.candidateType || "desconhecido";
            }
        });
        return tipoLocal;
    } catch (e) {
        console.warn("Erro ao verificar getStats:", e);
        return "erro";
    }
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
            // [Fix.4/C4] Distinguir "caiu depois de conectar" de "nunca conectou".
            const entrada = peers.get(sessaoId);
            const estavaConectado = !!(entrada && entrada.everConnected);
            const sessaoExiste = peers.has(sessaoId);
            fecharPeer(sessaoId);
            if (sessaoExiste) {
                window.dispatchEvent(new CustomEvent("physicalcloud:aviso-local", {
                    detail: {
                        sessaoId,
                        motivo: estavaConectado ? "conexao-perdida" : "conexao-nao-estabelecida",
                    },
                }));
                api.encerrarSessao(sessaoId);
            }
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

            // [Fix.4/C5] Verificação assíncrona pós-abertura — não bloqueia canal-aberto.
            setTimeout(async () => {
                const ent = peers.get(sessaoId);
                if (!ent) return;
                const tipo = await verificarConexaoDireta(ent.pc);
                if (tipo === "relay") {
                    window.dispatchEvent(new CustomEvent("physicalcloud:aviso-local", {
                        detail: { sessaoId, motivo: "relay-detectado" },
                    }));
                }
                window.dispatchEvent(new CustomEvent("physicalcloud:conexao-info", {
                    detail: { sessaoId, tipo },
                }));
            }, 1000);
        }
    }

    dc.onopen = () => {
        atualizarEstadoPeer(sessaoId, "canal-aberto");
        // [Fix.4/C4] Marca que o canal chegou a abrir para distinguir
        // "perdeu conexão" de "nunca conseguiu conectar".
        const ent = peers.get(sessaoId);
        if (ent) ent.everConnected = true;
        try {
            dc.send(JSON.stringify({ type: "pc-ok" }));
            handshakeEnviado = true;
        } catch (e) {
            console.error("Falha ao enviar handshake:", e);
        }
        verificar();
    };

    dc.addEventListener("message", (event) => {
        if (typeof event.data === "string") {
            let msg;
            try { msg = JSON.parse(event.data); } catch { msg = null; }
            if (msg && msg.type === "pc-ok") {
                handshakeRecebido = true;
                verificar();
                return;
            }
        }
    });

    dc.onclose = () => {
        atualizarEstadoPeer(sessaoId, "canal-fechado");
    };

    dc.onerror = (event) => {
        console.error(`DataChannel erro (sessão ${sessaoId}):`, event);
        atualizarEstadoPeer(sessaoId, "canal-erro");
    };
}

/**
 * Esta ponta é o CLIENTE.
 * @param {string} sessaoId
 */
async function iniciarComoCliente(sessaoId) {
    if (peers.has(sessaoId)) return;

    const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
    /** @type {EntradaPeer} */
    const entrada = { pc, dc: null, estado: "iniciando", sessaoId, candidatosPendentes: [], descricaoRemotaDefinida: false, everConnected: false };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    const dc = pc.createDataChannel(NOME_CANAL, { ordered: true });
    configurarDc(dc, sessaoId);

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
 * Esta ponta é o SERVIDOR.
 * @param {string} sessaoId
 */
function prepararComoServidor(sessaoId) {
    if (peers.has(sessaoId)) return;

    const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
    /** @type {EntradaPeer} */
    const entrada = { pc, dc: null, estado: "aguardando-offer", sessaoId, candidatosPendentes: [], descricaoRemotaDefinida: false, everConnected: false };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    pc.ondatachannel = (event) => {
        configurarDc(event.channel, sessaoId);
    };
}

/**
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
            entrada.descricaoRemotaDefinida = true;
            await processarCandidatosPendentes(entrada);

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
            entrada.descricaoRemotaDefinida = true;
            await processarCandidatosPendentes(entrada);

            atualizarEstadoPeer(sessaoId, "conectando");
        } catch (e) {
            console.error("Falha ao processar answer:", e);
            fecharPeer(sessaoId);
        }
        return;
    }

    if (dados.tipo === "ice") {
        if (!dados.candidate) return;
        if (!entrada.descricaoRemotaDefinida) {
            entrada.candidatosPendentes.push(dados.candidate);
            return;
        }
        try {
            await pc.addIceCandidate(new RTCIceCandidate(dados.candidate));
        } catch (e) {
            console.warn("Falha ao adicionar ICE candidate:", e);
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