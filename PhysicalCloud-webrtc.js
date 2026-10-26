// @ts-check

const STUN_SERVERS = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun.cloudflare.com:3478" },
];

const NOME_CANAL = "physicalcloud";

/**
 * @typedef {{ pc: RTCPeerConnection, dc: RTCDataChannel | null, estado: string, sessaoId: string, candidatosPendentes: RTCIceCandidateInit[], descricaoRemotaDefinida: boolean }} EntradaPeer
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
    // Remove de peers ANTES de chamar pc.close() para que qualquer disparo síncrono
    // de onconnectionstatechange causado pelo close() encontre peers.has() === false
    // e não execute o bloco de cleanup novamente (sem loop infinito).
    // Nota (5ª auditoria): browsers geralmente não disparam connectionstatechange ao
    // chamar pc.close() quando o estado já é "failed", mas o guard acima cobre o caso
    // de implementações que o fazem para "closed", garantindo idempotência.
    peers.delete(sessaoId);
    try { entrada.dc?.close(); } catch { /* ignorar */ }
    try { entrada.pc.close(); } catch { /* ignorar */ }
}

/**
 * B2 CORRIGIDO: processa os ICE candidates que chegaram antes de setRemoteDescription terminar.
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
            // BUG MÉDIO CORRIGIDO (3ª auditoria): dispositivo local não via aviso ao perder conexão.
            //
            // Problema: api.encerrarSessao() chama sessoes.delete() localmente antes de enviar
            // "encerrar" ao servidor. Quando o servidor responde "sessao-encerrada", o handler
            // em PhysicalCloud.js encontra existia = false (já deletado) e não mostra aviso.
            // Resultado: apenas o outro lado via a mensagem de encerramento — não o local.
            //
            // Solução: emite physicalcloud:aviso-local aqui (lado local que detectou a falha),
            // ANTES de chamar encerrarSessao. PhysicalCloud.js ouve este evento e chama aviso().
            // O handler de "sessao-encerrada" também pode chamar aviso() mas para motivos
            // diferentes ("encerrado-pelo-dispositivo" → outro lado; este é o lado local).
            // sessaoExiste calculado ANTES de fecharPeer — fecharPeer deleta de peers
            // internamente antes de chamar pc.close(), então peers.has() seria false depois.
            const sessaoExiste = peers.has(sessaoId);
            fecharPeer(sessaoId);
            if (sessaoExiste) {
                // Aviso explícito para o lado local (quem detectou a falha WebRTC)
                window.dispatchEvent(new CustomEvent("physicalcloud:aviso-local", {
                    detail: { sessaoId, motivo: "conexao-perdida" },
                }));
                // Notifica servidor — quando "sessao-encerrada" vier de volta,
                // peers.has() === false então fecharPeer não roda de novo (sem double).
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

    // BUG 6 (MÉDIO) CORRIGIDO: usando addEventListener em vez de dc.onmessage.
    //
    // Problema anterior: PhysicalCloud-transfer.js sobrescrevia dc.onmessage ao receber
    // "physicalcloud:canal-aberto" (disparado dentro de verificar() acima). Isso criava
    // uma dependência de ordem: se o listener de transfer.js fosse registrado antes de
    // configurarDc terminar, o onmessage original daqui seria perdido ou vice-versa.
    //
    // Solução: addEventListener("message") permite múltiplos handlers independentes.
    // Este handler trata apenas "pc-ok" e delega o restante via CustomEvent.
    // O handler de transfer.js adiciona seu próprio listener separado.
    dc.addEventListener("message", (event) => {
        if (typeof event.data === "string") {
            let msg;
            try { msg = JSON.parse(event.data); } catch { msg = null; }
            if (msg && msg.type === "pc-ok") {
                handshakeRecebido = true;
                verificar();
                return;
            }
            // Não re-despacha como CustomEvent — transfer.js usa seu próprio addEventListener
            // e recebe a mensagem diretamente do dc. Evita despachar duas vezes.
        }
        // ArrayBuffer: transfer.js trata diretamente via seu listener.
    });

    dc.onclose = () => {
        atualizarEstadoPeer(sessaoId, "canal-fechado");
    };

    // Nota de extensibilidade (4ª auditoria): o evento genérico "physicalcloud:mensagem"
    // que existia em versões anteriores foi removido intencionalmente. Ele re-despachava
    // toda mensagem do DataChannel como CustomEvent, criando overhead e acoplamento.
    //
    // Módulos futuros (Bloco C e além) que precisem ouvir mensagens do canal devem:
    //   1. Ouvir "physicalcloud:canal-aberto" para obter referência ao dc.
    //   2. Registrar seu próprio dc.addEventListener("message", handler) diretamente.
    //   3. Filtrar pelo prefixo/type relevante (ex.: msg.type.startsWith("chat-")).
    //
    // Este padrão evita intermediários e garante que cada módulo trate exatamente
    // as mensagens que lhe pertencem, sem dependência de ordem de carregamento.

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
    const entrada = { pc, dc: null, estado: "iniciando", sessaoId, candidatosPendentes: [], descricaoRemotaDefinida: false };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    const dc = pc.createDataChannel(NOME_CANAL, { ordered: true });
    // BUG 6 CORRIGIDO (versão anterior): configurarDc já faz entrada.dc = dc internamente.
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
 * Esta ponta é o SERVIDOR — cria RTCPeerConnection e aguarda offer via sinal.
 * @param {string} sessaoId
 */
function prepararComoServidor(sessaoId) {
    if (peers.has(sessaoId)) return;

    const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
    /** @type {EntradaPeer} */
    const entrada = { pc, dc: null, estado: "aguardando-offer", sessaoId, candidatosPendentes: [], descricaoRemotaDefinida: false };
    peers.set(sessaoId, entrada);

    configurarPc(pc, sessaoId);

    pc.ondatachannel = (event) => {
        // BUG 4 (BAIXO) CORRIGIDO: entrada.dc = event.channel era redundante aqui.
        // configurarDc() já faz entrada.dc = dc internamente na primeira linha.
        // A atribuição extra depois não causava bug visível mas era código enganoso.
        configurarDc(event.channel, sessaoId);
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
            entrada.descricaoRemotaDefinida = true;
            // B2: processa candidates que chegaram antes do offer
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
            // B2: processa candidates que chegaram antes do answer
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
            // B2: descrição remota ainda não chegou — guarda para depois
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