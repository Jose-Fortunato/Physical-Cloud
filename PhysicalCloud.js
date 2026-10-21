// @ts-check

const URL_SERVIDOR = "wss://physical-cloud-server.onrender.com";
const TEMPO_PEDIDO_S = 60;

const telaDeCodigo        = document.getElementById("modal-codigo");
const codigoTexto         = document.getElementById("codigo-texto");
const fecharModal         = document.getElementById("btn-fechar-modal");
const telaDeConectar      = document.getElementById("modal-conectar");
const fecharModalConectar = document.getElementById("btn-fechar-conectar");

/** @type {HTMLInputElement[]} */
const inputs = Array.from(document.querySelectorAll("input.code-input-box"));

/** @type {WebSocket | null} */
let socket = null;

/** @type {string | null} */
let meuCodigoServidor = null;

let souServidor = false;
let abrirModalAoGerar = false;
let aguardandoAprovacao = false;

/** @type {string | null} */
let sessaoAguardando = null;

/** @type {Map<string, {papel: string, nome: string, permissao: string}>} */
const sessoes = new Map();

/** @type {ReturnType<typeof setInterval> | null} */
let pingInterval = null;
/** @type {ReturnType<typeof setTimeout> | null} */
let timerReconexao = null;
let tentativasReconexao = 0;
let reconectando = false;


// ─── Identificação do dispositivo ────────────────────────────────────────────

/** @type {string | null} */
let deviceIdMemoria = null;

function gerarId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
}

function obterDeviceId() {
    if (deviceIdMemoria) return deviceIdMemoria;

    /** @type {string | null} */
    let id = null;
    try { id = localStorage.getItem("physicalCloudDeviceId"); } catch { /* navegação privada */ }

    if (!id || !/^[A-Za-z0-9_-]{8,64}$/.test(id)) {
        id = gerarId();
        try { localStorage.setItem("physicalCloudDeviceId", id); } catch { /* sem armazenamento */ }
    }
    deviceIdMemoria = id;
    return id;
}

function obterNomeDispositivo() {
    const ua = navigator.userAgent;
    if (/Android/i.test(ua)) return "Celular Android";
    if (/iPhone|iPad/i.test(ua)) return "iPhone/iPad";
    if (/Windows/i.test(ua)) return "PC Windows";
    if (/Mac/i.test(ua)) return "Mac";
    if (/Linux/i.test(ua)) return "PC Linux";
    return "Dispositivo";
}


// ─── Envio e avisos na tela ──────────────────────────────────────────────────

/**
 * @param {{ type: string; deviceId?: string; sessaoId?: any; aceitar?: any; permissao?: any; codigo?: string; nome?: string; }} obj
 */
function enviar(obj) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    socket.send(JSON.stringify(obj));
    return true;
}

/** @type {HTMLElement | null} */
let areaAvisos = null;

/**
 * @param {string | null} texto
 */
function aviso(texto) {
    if (!areaAvisos) {
        areaAvisos = document.createElement("div");
        Object.assign(areaAvisos.style, {
            position: "fixed", left: "50%", bottom: "24px", transform: "translateX(-50%)",
            display: "flex", flexDirection: "column", gap: "8px", zIndex: "10000",
            maxWidth: "90vw", pointerEvents: "none",
        });
        document.body.appendChild(areaAvisos);
    }
    const el = document.createElement("div");
    el.textContent = texto;
    Object.assign(el.style, {
        background: "#1b1b1b", color: "#fff", border: "1px solid #1f7a4d",
        borderRadius: "10px", padding: "10px 16px", fontSize: "14px", textAlign: "center",
    });
    areaAvisos.appendChild(el);
    setTimeout(() => el.remove(), 4500);
}

const MOTIVOS_ENCERRAMENTO = {
    "recusado": "O dispositivo recusou a conexão.",
    "pedido-expirou": "O pedido de conexão expirou sem resposta.",
    "dispositivo-desconectou": "O outro dispositivo foi desconectado.",
    "encerrado-pelo-dispositivo": "O outro dispositivo encerrou a conexão.",
};

const TEXTO_PERMISSAO = {
    "leitura": "só leitura",
    "leitura-escrita": "leitura e escrita",
};


// ─── Lista de dispositivos ────────────────────────────────────────────────────

const listaDispositivos = document.getElementById("lista-dispositivos");
const emptyDispositivos = document.getElementById("empty-dispositivos");
const deviceActions     = document.getElementById("device-actions");

function atualizarListaDispositivos() {
    if (!listaDispositivos || !emptyDispositivos) return;

    listaDispositivos.innerHTML = "";

    if (sessoes.size === 0) {
        emptyDispositivos.style.display = "";
        listaDispositivos.style.display = "none";
        if (deviceActions) deviceActions.style.display = "none";
        return;
    }

    emptyDispositivos.style.display = "none";
    listaDispositivos.style.display = "";
    if (deviceActions) deviceActions.style.display = "";

    for (const [sessaoId, s] of sessoes) {
        const card = document.createElement("div");
        card.className = "device-card";
        card.dataset.sessaoId = sessaoId;

        const icone = document.createElement("div");
        icone.className = "device-icon";
        icone.textContent = s.papel === "servidor" ? "💻" : "📱";

        const info = document.createElement("div");
        info.className = "device-info";

        const nome = document.createElement("span");
        nome.className = "device-name";
        nome.textContent = s.nome;

        const detalhes = document.createElement("span");
        detalhes.className = "device-details";
        // @ts-ignore
        const permissaoTexto = TEXTO_PERMISSAO[s.permissao] || s.permissao;
        detalhes.textContent = s.papel === "servidor"
            ? `Conectado a você · ${permissaoTexto}`
            : `Você conectado · ${permissaoTexto}`;

        info.append(nome, detalhes);

        const badge = document.createElement("span");
        badge.className = "device-badge device-badge--online";
        badge.textContent = "Online";

        const btnEncerrar = document.createElement("button");
        btnEncerrar.className = "device-btn-encerrar";
        btnEncerrar.textContent = "Desconectar";
        btnEncerrar.addEventListener("click", () => {
            encerrarSessao(sessaoId);
        });

        card.append(icone, info, badge, btnEncerrar);
        listaDispositivos.appendChild(card);
    }
}


// ─── Abrir modal de conectar ──────────────────────────────────────────────────

function abrirModalConectar() {
    limparInputs();
    telaDeConectar?.classList.add("active");
    inputs[0]?.focus();
}


// ─── Tabs ─────────────────────────────────────────────────────────────────────

document.querySelectorAll(".nav-item[data-tab]").forEach(btn => {
    btn.addEventListener("click", () => {
        const alvo = /** @type {HTMLElement} */ (btn).dataset.tab;
        if (!alvo) return;

        document.querySelectorAll(".nav-item").forEach(b => b.classList.remove("active"));
        document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));

        btn.classList.add("active");
        document.getElementById(alvo)?.classList.add("active");
    });
});


// ─── Contagem regressiva do código ───────────────────────────────────────────

/** @type {HTMLElement | null} */
let elContagem = null;
/** @type {HTMLButtonElement | null} */
let btnNovoCodigo = null;
let fimCodigo = 0;
/** @type {ReturnType<typeof setInterval> | null} */
let timerCodigo = null;

function prepararExtrasDoModalCodigo() {
    if (!codigoTexto || elContagem) return;

    elContagem = document.createElement("p");
    elContagem.id = "codigo-contagem";
    Object.assign(elContagem.style, { opacity: "0.8", fontSize: "14px", margin: "18px 0 8px" });

    btnNovoCodigo = document.createElement("button");
    btnNovoCodigo.type = "button";
    btnNovoCodigo.id = "btn-novo-codigo";
    btnNovoCodigo.textContent = "Gerar novo código";
    Object.assign(btnNovoCodigo.style, {
        background: "transparent", color: "inherit", border: "1px solid #1f7a4d",
        borderRadius: "8px", padding: "6px 14px", cursor: "pointer", display: "block", margin: "8px auto 0",
    });
    btnNovoCodigo.addEventListener("click", () => {
        abrirModalAoGerar = true;
        if (!enviar({ type: "tornar-servidor", deviceId: obterDeviceId() })) {
            abrirModalAoGerar = false;
            aviso("Servidor desconectado. Aguarde a reconexão.");
        }
    });

    codigoTexto.parentElement?.append(elContagem, btnNovoCodigo);
}

function mostrarDigitos(codigo = "") {
    const caixas = document.querySelectorAll("#modal-codigo .code-digit");
    caixas.forEach((caixa, i) => { caixa.textContent = codigo[i] || ""; });
}

function pararContagem() {
    if (timerCodigo) clearInterval(timerCodigo);
    timerCodigo = null;
}

function atualizarContagem() {
    const restante = Math.max(0, Math.ceil((fimCodigo - Date.now()) / 1000));
    if (elContagem) {
        if (restante > 0) {
            const m = Math.floor(restante / 60);
            const s = String(restante % 60).padStart(2, "0");
            elContagem.textContent = `Expira em ${m}:${s}`;
        } else {
            elContagem.textContent = "Código expirado. Gere um novo.";
        }
    }
    if (restante === 0) pararContagem();
}

/**
 * @param {number} segundos
 */
function iniciarContagem(segundos) {
    pararContagem();
    fimCodigo = Date.now() + segundos * 1000;
    atualizarContagem();
    timerCodigo = setInterval(atualizarContagem, 1000);
}


// ─── Pedidos de conexão ───────────────────────────────────────────────────────

/** @type {{sessaoId: string, nome: string, chegouEm: number}[]} */
const filaPedidos = [];
/** @type {HTMLElement | null} */
let dialogoPedido = null;
/** @type {ReturnType<typeof setInterval> | null} */
let timerPedido = null;

function fecharDialogoPedido() {
    if (timerPedido) clearInterval(timerPedido);
    timerPedido = null;
    if (dialogoPedido) dialogoPedido.remove();
    dialogoPedido = null;
}

/**
 * @param {boolean} aceitar
 * @param {string} permissao
 */
function responderPedido(aceitar, permissao) {
    const pedido = filaPedidos.shift();
    if (!pedido) return;
    enviar({ type: "responder-pedido", sessaoId: pedido.sessaoId, aceitar, permissao });
    fecharDialogoPedido();
    mostrarProximoPedido();
}

function mostrarProximoPedido() {
    if (dialogoPedido) return;
    const pedido = filaPedidos[0];
    if (!pedido) return;

    const fundo = document.createElement("div");
    Object.assign(fundo.style, {
        position: "fixed", inset: "0", background: "rgba(0,0,0,0.65)", display: "flex",
        alignItems: "center", justifyContent: "center", zIndex: "9999",
    });

    const caixa = document.createElement("div");
    Object.assign(caixa.style, {
        background: "#111", color: "#fff", border: "1px solid #2a2a2a", borderRadius: "16px",
        padding: "24px", width: "min(380px, 90vw)", textAlign: "center",
    });

    const titulo = document.createElement("h3");
    titulo.textContent = "Permitir conexão?";
    titulo.style.margin = "0 0 12px";

    const texto = document.createElement("p");
    texto.textContent = `"${pedido.nome}" quer acessar a pasta deste dispositivo.`;

    const contagem = document.createElement("p");
    Object.assign(contagem.style, { opacity: "0.7", fontSize: "13px" });

    const rotulo = document.createElement("label");
    rotulo.textContent = "Permissão: ";
    const escolha = document.createElement("select");
    for (const [valor, nome] of Object.entries(TEXTO_PERMISSAO)) {
        const opcao = document.createElement("option");
        opcao.value = valor;
        opcao.textContent = nome;
        escolha.appendChild(opcao);
    }
    rotulo.appendChild(escolha);

    const botoes = document.createElement("div");
    Object.assign(botoes.style, { display: "flex", gap: "12px", justifyContent: "center", marginTop: "16px" });

    const btnPermitir = document.createElement("button");
    btnPermitir.type = "button";
    btnPermitir.textContent = "Permitir";
    Object.assign(btnPermitir.style, {
        background: "#1f7a4d", color: "#fff", border: "none", borderRadius: "10px",
        padding: "10px 20px", cursor: "pointer",
    });
    btnPermitir.addEventListener("click", () => responderPedido(true, escolha.value));

    const btnRecusar = document.createElement("button");
    btnRecusar.type = "button";
    btnRecusar.textContent = "Recusar";
    Object.assign(btnRecusar.style, {
        background: "#2a2a2a", color: "#fff", border: "none", borderRadius: "10px",
        padding: "10px 20px", cursor: "pointer",
    });
    btnRecusar.addEventListener("click", () => responderPedido(false, "leitura"));

    botoes.append(btnPermitir, btnRecusar);
    caixa.append(titulo, texto, rotulo, contagem, botoes);
    fundo.appendChild(caixa);
    document.body.appendChild(fundo);
    dialogoPedido = fundo;

    const atualizar = () => {
        const restante = Math.max(0, TEMPO_PEDIDO_S - Math.floor((Date.now() - pedido.chegouEm) / 1000));
        contagem.textContent = `Expira em ${restante}s`;
    };
    atualizar();
    timerPedido = setInterval(atualizar, 1000);
}

/**
 * @param {string} sessaoId
 */
function removerPedido(sessaoId) {
    const posicao = filaPedidos.findIndex(p => p.sessaoId === sessaoId);
    if (posicao === -1) return;
    const eraOAberto = posicao === 0 && dialogoPedido !== null;
    filaPedidos.splice(posicao, 1);
    if (eraOAberto) {
        fecharDialogoPedido();
        mostrarProximoPedido();
    }
}


// ─── Inputs do código ─────────────────────────────────────────────────────────

function limparInputs() {
    inputs.forEach(input => { input.value = ""; });
}

function enviarCodigoSeCompleto() {
    if (!inputs.every(input => input.value !== "")) return;
    if (aguardandoAprovacao) return;

    const codigo = inputs.map(input => input.value).join("");

    if (meuCodigoServidor !== null && codigo === meuCodigoServidor) {
        aviso("Você não pode conectar ao próprio dispositivo servidor.");
        limparInputs();
        inputs[0].focus();
        return;
    }

    const enviado = enviar({
        type: "conectar",
        codigo: codigo,
        deviceId: obterDeviceId(),
        nome: obterNomeDispositivo(),
    });
    if (!enviado) {
        aviso("O servidor está desconectado. Aguarde a reconexão e tente de novo.");
    }
}

inputs.forEach((input, index) => {
    input.addEventListener("input", () => {
        input.value = input.value.replace(/\D/g, "").slice(0, 1);
        if (input.value && index < inputs.length - 1) inputs[index + 1].focus();
        enviarCodigoSeCompleto();
    });

    input.addEventListener("keydown", event => {
        if (event.key === "Backspace" && input.value === "" && index > 0) {
            inputs[index - 1].focus();
        }
    });

    input.addEventListener("paste", event => {
        const clipboardData = event.clipboardData;
        if (!clipboardData) return;
        const colado = clipboardData.getData("text").replace(/\D/g, "").slice(0, 6);
        if (!colado) return;
        event.preventDefault();
        limparInputs();
        colado.split("").forEach((digito, i) => { if (inputs[i]) inputs[i].value = digito; });
        const proximoFoco = Math.min(colado.length, inputs.length - 1);
        inputs[proximoFoco].focus();
        enviarCodigoSeCompleto();
    });
});


// ─── Botões principais ────────────────────────────────────────────────────────

document.getElementById("btn-tornar-servidor")?.addEventListener("click", () => {
    abrirModalAoGerar = true;
    if (!enviar({ type: "tornar-servidor", deviceId: obterDeviceId() })) {
        abrirModalAoGerar = false;
        aviso("Servidor desconectado. Aguarde a reconexão.");
        return;
    }
    souServidor = true;
    telaDeCodigo?.classList.remove("active");
});

document.getElementById("btn-conectar")?.addEventListener("click", abrirModalConectar);
document.getElementById("btn-conectar-extra")?.addEventListener("click", abrirModalConectar);

fecharModalConectar?.addEventListener("click", () => {
    telaDeConectar?.classList.remove("active");
    limparInputs();
});

fecharModal?.addEventListener("click", () => {
    telaDeCodigo?.classList.remove("active");
});

document.getElementById("btn-enviar")?.addEventListener("click", () => {
    aviso("Transferência de arquivos será implementada na próxima etapa.");
});


// ─── Mensagens recebidas do servidor WebSocket ────────────────────────────────

/**
 * @param {{ data: string; }} event
 */
function tratarMensagem(event) {
    let dados;
    try {
        dados = JSON.parse(event.data);
    } catch {
        console.error("Mensagem inválida do servidor:", event.data);
        return;
    }
    if (!dados || typeof dados !== "object") return;

    switch (dados.type) {

        case "codigo-gerado":
            meuCodigoServidor = String(dados.codigo);
            if (codigoTexto) codigoTexto.textContent = meuCodigoServidor;
            mostrarDigitos(meuCodigoServidor);
            prepararExtrasDoModalCodigo();
            iniciarContagem(Number(dados.validadeSegundos) || 300);
            if (abrirModalAoGerar) telaDeCodigo?.classList.add("active");
            abrirModalAoGerar = false;
            break;

        case "codigo-expirado":
            meuCodigoServidor = null;
            pararContagem();
            mostrarDigitos("");
            if (elContagem) elContagem.textContent = "Código expirado. Gere um novo.";
            aviso("Seu código expirou. Gere um novo para aceitar outros dispositivos.");
            break;

        case "aguardando-aprovacao":
            aguardandoAprovacao = true;
            sessaoAguardando = String(dados.sessaoId);
            aviso("Pedido enviado. Aguarde o outro dispositivo aprovar…");
            break;

        case "pedido-conexao":
            filaPedidos.push({
                sessaoId: String(dados.sessaoId),
                nome: String(dados.nome || "Dispositivo"),
                chegouEm: Date.now(),
            });
            mostrarProximoPedido();
            break;

        case "conectado": {
            aguardandoAprovacao = false;
            sessaoAguardando = null;
            const permissao = String(dados.permissao || "leitura");
            sessoes.set(String(dados.sessaoId), { papel: "cliente", nome: "Dispositivo servidor", permissao });
            telaDeConectar?.classList.remove("active");
            limparInputs();
            // @ts-ignore
            aviso(`${dados.mensagem || "Conexão estabelecida com sucesso!"} (${TEXTO_PERMISSAO[permissao] || permissao})`);
            atualizarListaDispositivos();
            break;
        }

        case "dispositivo-conectou": {
            const permissao = String(dados.permissao || "leitura");
            const nome = String(dados.nome || "Dispositivo");
            sessoes.set(String(dados.sessaoId), { papel: "servidor", nome, permissao });
            // @ts-ignore
            aviso(`${nome} conectou ao seu dispositivo (${TEXTO_PERMISSAO[permissao] || permissao}).`);
            atualizarListaDispositivos();
            break;
        }

        case "sessao-encerrada": {
            const id = String(dados.sessaoId);
            removerPedido(id);
            const existia = sessoes.delete(id);
            const eraMeuPedido = id === sessaoAguardando;
            if (eraMeuPedido) {
                aguardandoAprovacao = false;
                sessaoAguardando = null;
                limparInputs();
                inputs[0]?.focus();
            }
            if (existia || eraMeuPedido) {
                // @ts-ignore
                aviso(MOTIVOS_ENCERRAMENTO[dados.motivo] || "A conexão foi encerrada.");
            }
            atualizarListaDispositivos();
            break;
        }

        case "sinal":
            window.dispatchEvent(new CustomEvent("physicalcloud:sinal", { detail: dados }));
            break;

        case "erro":
            aguardandoAprovacao = false;
            sessaoAguardando = null;
            aviso(dados.mensagem || "Não foi possível conectar.");
            limparInputs();
            inputs[0]?.focus();
            break;

        default:
            console.log("Mensagem não tratada:", dados);
    }
}


// ─── Conexão WebSocket (com reconexão automática) ─────────────────────────────

function conectarSocket() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    if (timerReconexao) clearTimeout(timerReconexao);
    timerReconexao = null;

    const ws = new WebSocket(URL_SERVIDOR);
    socket = ws;

    ws.addEventListener("open", () => {
        tentativasReconexao = 0;

        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "ping" }));
        }, 240000);

        if (reconectando) {
            reconectando = false;
            aviso("Reconectado ao servidor.");
        }

        if (souServidor) {
            abrirModalAoGerar = false;
            enviar({ type: "tornar-servidor", deviceId: obterDeviceId() });
        }
    });

    ws.addEventListener("message", tratarMensagem);

    ws.addEventListener("error", () => {
        console.error("Erro no WebSocket.");
    });

    ws.addEventListener("close", event => {
        if (socket !== ws) return;

        if (pingInterval) clearInterval(pingInterval);
        pingInterval = null;

        meuCodigoServidor = null;
        pararContagem();
        mostrarDigitos("");
        telaDeCodigo?.classList.remove("active");

        sessoes.clear();
        atualizarListaDispositivos();

        filaPedidos.length = 0;
        fecharDialogoPedido();
        aguardandoAprovacao = false;
        sessaoAguardando = null;

        const recusado = event.code === 1008;
        const espera = recusado ? 30000 : Math.min(1000 * 2 ** tentativasReconexao, 15000);
        if (tentativasReconexao === 0 || recusado) {
            aviso(recusado
                ? `O servidor recusou a conexão${event.reason ? ": " + event.reason : ""}.`
                : "Conexão perdida. Tentando reconectar…");
        }
        tentativasReconexao++;
        reconectando = true;
        timerReconexao = setTimeout(conectarSocket, espera);
    });
}

document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    if (socket && socket.readyState !== WebSocket.CLOSED) return;
    if (timerReconexao) clearTimeout(timerReconexao);
    tentativasReconexao = 0;
    conectarSocket();
});


// ─── API para etapas futuras (WebRTC) ─────────────────────────────────────────

/**
 * @param {string} sessaoId
 */
function encerrarSessao(sessaoId) {
    enviar({ type: "encerrar", sessaoId });
    sessoes.delete(sessaoId);
    atualizarListaDispositivos();
}

/** @type {any} */ (window).PhysicalCloud = { enviar, sessoes, obterDeviceId, encerrarSessao };

conectarSocket();