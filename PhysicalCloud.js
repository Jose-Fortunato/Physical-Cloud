// @ts-check

const URL_SERVIDOR = "wss://physical-cloud-server.onrender.com";
const TEMPO_PEDIDO_S = 60; // o servidor expira pedidos sem resposta em 60s

// ─── Referências aos elementos HTML ──────────────────────────────────────────
const telaDeCodigo        = document.getElementById("modal-codigo");        // Modal que mostra o código gerado
const codigoTexto         = document.getElementById("codigo-texto");         // Elemento onde o código é exibido
const fecharModal         = document.getElementById("btn-fechar-modal");     // Botão de fechar o modal do código
const telaDeConectar      = document.getElementById("modal-conectar");      // Modal para digitar o código de outro dispositivo
const fecharModalConectar = document.getElementById("btn-fechar-conectar");  // Botão de fechar o modal de conexão

// Todos os inputs de dígito do código (cada caixinha = 1 dígito)
/** @type {HTMLInputElement[]} */
const inputs = Array.from(document.querySelectorAll("input.code-input-box"));


// ─── Estado ──────────────────────────────────────────────────────────────────

/** @type {WebSocket | null} */
let socket = null;

// Código do próprio dispositivo quando ele vira servidor (evita auto-conexão)
/** @type {string | null} */
let meuCodigoServidor = null;

// true depois que o usuário clica em "Se tornar o dispositivo servidor".
// Serve para pedir um código novo sozinho se a conexão cair e voltar
// (o servidor esquece tudo quando reinicia).
let souServidor = false;

// Se o modal do código deve abrir quando o código chegar
let abrirModalAoGerar = false;

// true enquanto espero o dispositivo servidor aprovar meu pedido
let aguardandoAprovacao = false;
/** @type {string | null} */
let sessaoAguardando = null;

// Conexões ativas: sessaoId → { papel: "servidor" | "cliente", nome, permissao }
// "servidor" = alguém está conectado a MIM; "cliente" = eu estou conectado a alguém
/** @type {Map<string, {papel: string, nome: string, permissao: string}>} */
const sessoes = new Map();

/** @type {ReturnType<typeof setInterval> | null} */
let pingInterval = null;
/** @type {ReturnType<typeof setTimeout> | null} */
let timerReconexao = null;
let tentativasReconexao = 0;
let reconectando = false;


// ─── Identificação do dispositivo ────────────────────────────────────────────
// O deviceId é gerado uma vez e guardado no navegador. O servidor usa isso para
// impedir que um dispositivo conecte no próprio código (mesmo em outra aba).
// Obs.: navegadores diferentes no mesmo aparelho têm IDs diferentes.

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

// Nome que aparece para o outro lado no pedido de conexão
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

/** Envia um objeto JSON ao servidor. Retorna false se o socket não estiver aberto. */
// @ts-ignore
function enviar(obj) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    socket.send(JSON.stringify(obj));
    return true;
}

/** @type {HTMLElement | null} */
let areaAvisos = null;

// Aviso rápido que some sozinho (não trava a tela como o alert)
// @ts-ignore
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
    el.textContent = texto; // textContent: nunca interpreta HTML
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


// ─── Contagem regressiva do código (modal do código) ─────────────────────────
/** @type {HTMLElement | null} */
let elContagem = null;
/** @type {HTMLButtonElement | null} */
let btnNovoCodigo = null;
let fimCodigo = 0;
/** @type {ReturnType<typeof setInterval> | null} */
let timerCodigo = null;

// Cria (uma vez) a contagem e o botão "Gerar novo código" dentro do modal do código
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

    // No fim do modal (abaixo das caixinhas do código)
    codigoTexto.parentElement?.append(elContagem, btnNovoCodigo);
}

// Escreve cada dígito do código dentro da sua caixinha (.code-digit).
// Sem argumento (ou ""), esvazia as caixinhas.
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

// @ts-ignore
function iniciarContagem(segundos) {
    pararContagem();
    fimCodigo = Date.now() + segundos * 1000; // usa o relógio local (não depende do servidor)
    atualizarContagem();
    timerCodigo = setInterval(atualizarContagem, 1000);
}


// ─── Pedidos de conexão (aprovação no dispositivo servidor) ──────────────────
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

// @ts-ignore
function responderPedido(aceitar, permissao) {
    const pedido = filaPedidos.shift();
    if (!pedido) return;
    enviar({ type: "responder-pedido", sessaoId: pedido.sessaoId, aceitar, permissao });
    fecharDialogoPedido();
    mostrarProximoPedido();
}

// Mostra um pedido por vez. Os outros esperam na fila.
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

// Um pedido (ou conexão) foi encerrado: tira da fila / da tela
// @ts-ignore
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


// ─── Utilitários dos inputs ──────────────────────────────────────────────────

function limparInputs() {
    inputs.forEach(input => { input.value = ""; });
}

// Verifica se todos os dígitos foram preenchidos e, se sim, envia o código ao servidor
function enviarCodigoSeCompleto() {
    if (!inputs.every(input => input.value !== "")) return;
    if (aguardandoAprovacao) return; // já tem um pedido esperando resposta

    const codigo = inputs.map(input => input.value).join("");

    // Bloqueia auto-conexão já no cliente
    if (meuCodigoServidor !== null && codigo === meuCodigoServidor) {
        alert("Você não pode conectar ao próprio dispositivo servidor.");
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
        alert("O servidor está desconectado. Aguarde a reconexão e tente de novo.");
    }
}


// ─── Eventos dos inputs de código ────────────────────────────────────────────
inputs.forEach((input, index) => {

    // Ao digitar: só números, avança para o próximo campo e verifica se está completo
    input.addEventListener("input", () => {
        input.value = input.value.replace(/\D/g, "").slice(0, 1);

        if (input.value && index < inputs.length - 1) {
            inputs[index + 1].focus();
        }

        enviarCodigoSeCompleto();
    });

    // Backspace em campo vazio: volta para o campo anterior
    input.addEventListener("keydown", event => {
        if (event.key === "Backspace" && input.value === "" && index > 0) {
            inputs[index - 1].focus();
        }
    });

    // Ao colar: distribui os dígitos entre os inputs e tenta enviar
    input.addEventListener("paste", event => {
        const clipboardData = event.clipboardData;
        if (!clipboardData) return;

        const colado = clipboardData.getData("text").replace(/\D/g, "").slice(0, 6);
        if (!colado) return;

        event.preventDefault();
        limparInputs();

        colado.split("").forEach((digito, i) => {
            if (inputs[i]) inputs[i].value = digito;
        });

        const proximoFoco = Math.min(colado.length, inputs.length - 1);
        inputs[proximoFoco].focus();

        enviarCodigoSeCompleto();
    });
});


// ─── Botões principais ────────────────────────────────────────────────────────

// "Se tornar o dispositivo servidor": solicita um código ao servidor
document.getElementById("btn-tornar-servidor")?.addEventListener("click", () => {
    abrirModalAoGerar = true;
    if (!enviar({ type: "tornar-servidor", deviceId: obterDeviceId() })) {
        abrirModalAoGerar = false;
        aviso("Servidor desconectado. Aguarde a reconexão.");
        return;
    }
    souServidor = true;
    telaDeCodigo?.classList.remove("active"); // fecha o modal antigo até o código novo chegar
});

// "Conectar dispositivo": abre o modal para digitar o código
document.getElementById("btn-conectar")?.addEventListener("click", () => {
    limparInputs();
    telaDeConectar?.classList.add("active");
    inputs[0]?.focus();
});

fecharModalConectar?.addEventListener("click", () => {
    telaDeConectar?.classList.remove("active");
    limparInputs();
});

fecharModal?.addEventListener("click", () => {
    telaDeCodigo?.classList.remove("active");
});


// ─── Mensagens recebidas do servidor WebSocket ────────────────────────────────
// @ts-ignore
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

        // Meu código de servidor chegou
        case "codigo-gerado":
            meuCodigoServidor = String(dados.codigo);
            if (codigoTexto) codigoTexto.textContent = meuCodigoServidor; // fica escondido, só para leitores de tela
            mostrarDigitos(meuCodigoServidor);
            prepararExtrasDoModalCodigo();
            iniciarContagem(Number(dados.validadeSegundos) || 300);
            if (abrirModalAoGerar) telaDeCodigo?.classList.add("active");
            abrirModalAoGerar = false;
            break;

        // O código venceu (quem já estava conectado continua conectado)
        case "codigo-expirado":
            meuCodigoServidor = null;
            pararContagem();
            mostrarDigitos("");
            if (elContagem) elContagem.textContent = "Código expirado. Gere um novo.";
            aviso("Seu código expirou. Gere um novo para aceitar outros dispositivos.");
            break;

        // Meu pedido foi enviado; agora o dispositivo servidor decide
        case "aguardando-aprovacao":
            aguardandoAprovacao = true;
            sessaoAguardando = String(dados.sessaoId);
            aviso("Pedido enviado. Aguarde o outro dispositivo aprovar…");
            break;

        // Alguém quer conectar em MIM: pergunta se eu permito
        case "pedido-conexao":
            filaPedidos.push({
                sessaoId: String(dados.sessaoId),
                nome: String(dados.nome || "Dispositivo"),
                chegouEm: Date.now(),
            });
            mostrarProximoPedido();
            break;

        // Meu pedido foi aprovado
        case "conectado": {
            aguardandoAprovacao = false;
            sessaoAguardando = null;
            const permissao = String(dados.permissao || "leitura");
            sessoes.set(String(dados.sessaoId), { papel: "cliente", nome: "Dispositivo servidor", permissao });
            telaDeConectar?.classList.remove("active");
            limparInputs();
            // @ts-ignore
            aviso(`${dados.mensagem || "Conexão estabelecida com sucesso!"} (${TEXTO_PERMISSAO[permissao] || permissao})`);
            break;
        }

        // Aprovei alguém e a conexão foi concluída
        case "dispositivo-conectou": {
            const permissao = String(dados.permissao || "leitura");
            const nome = String(dados.nome || "Dispositivo");
            sessoes.set(String(dados.sessaoId), { papel: "servidor", nome, permissao });
            // @ts-ignore
            aviso(`${nome} conectou ao seu dispositivo (${TEXTO_PERMISSAO[permissao] || permissao}).`);
            break;
        }

        // Conexão ou pedido terminou (recusa, expiração, queda, desconexão)
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
            break;
        }

        // Mensagens de ligação (WebRTC). A transferência de arquivos (Dia 3) escuta este evento.
        case "sinal":
            window.dispatchEvent(new CustomEvent("physicalcloud:sinal", { detail: dados }));
            break;

        case "erro":
            aguardandoAprovacao = false;
            sessaoAguardando = null;
            alert(dados.mensagem || "Não foi possível conectar.");
            limparInputs();
            inputs[0]?.focus();
            break;

        default:
            console.log("Mensagem não tratada:", dados);
    }
}


// ─── Conexão com o servidor (com reconexão automática) ───────────────────────
function conectarSocket() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    if (timerReconexao) clearTimeout(timerReconexao);
    timerReconexao = null;

    const ws = new WebSocket(URL_SERVIDOR);
    socket = ws;

    ws.addEventListener("open", () => {
        tentativasReconexao = 0;

        // Ping periódico para manter o servidor do Render acordado
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "ping" }));
        }, 240000); // 4 minutos

        if (reconectando) {
            reconectando = false;
            aviso("Reconectado ao servidor.");
        }

        // O servidor esqueceu meu código: peço um novo, sem abrir o modal sozinho
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
        if (socket !== ws) return; // evento de um socket antigo

        if (pingInterval) clearInterval(pingInterval);
        pingInterval = null;

        // Tudo que dependia do servidor deixou de valer
        meuCodigoServidor = null;
        pararContagem();
        mostrarDigitos("");
        telaDeCodigo?.classList.remove("active");
        sessoes.clear();
        filaPedidos.length = 0;
        fecharDialogoPedido();
        aguardandoAprovacao = false;
        sessaoAguardando = null;

        // Tenta reconectar: 1s, 2s, 4s, 8s... no máximo 15s (30s se o servidor recusou)
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

// Ao voltar para a aba (comum no celular), reconecta na hora se tiver caído
document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    if (socket && socket.readyState !== WebSocket.CLOSED) return;
    if (timerReconexao) clearTimeout(timerReconexao);
    tentativasReconexao = 0;
    conectarSocket();
});


// ─── API para os próximos passos (transferência de arquivos) ─────────────────
// @ts-ignore
function encerrarSessao(sessaoId) {
    enviar({ type: "encerrar", sessaoId });
    sessoes.delete(sessaoId);
}

/** @type {any} */ (window).PhysicalCloud = { enviar, sessoes, obterDeviceId, encerrarSessao };

conectarSocket();