// @ts-check

// Abre a conexão WebSocket com o servidor remoto
const socket = new WebSocket('wss://physical-cloud-server.onrender.com');

// Ping periódico para manter o servidor do Render acordado
/** @type {ReturnType<typeof setInterval> | null} */
let pingInterval = null;

socket.addEventListener("open", () => {
    pingInterval = setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({ type: "ping" }));
        }
    }, 240000); // 4 minutos
});

// Se o socket cair, o código gerado deixa de valer no servidor: avisa o usuário
socket.addEventListener("close", () => {
    if (pingInterval) clearInterval(pingInterval);
    meuCodigoServidor = null;
    telaDeCodigo?.classList.remove("active");
    alert("Conexão com o servidor perdida. Recarregue a página e gere um novo código.");
});

socket.addEventListener("error", () => {
    console.error("Erro no WebSocket.");
});

// ─── Referências aos elementos HTML ──────────────────────────────────────────
const telaDeCodigo        = document.getElementById("modal-codigo");        // Modal que mostra o código gerado
const codigoTexto         = document.getElementById("codigo-texto");         // Elemento onde o código é exibido
const fecharModal         = document.getElementById("btn-fechar-modal");     // Botão de fechar o modal do código
const telaDeConectar      = document.getElementById("modal-conectar");      // Modal para digitar o código de outro dispositivo
const fecharModalConectar = document.getElementById("btn-fechar-conectar");  // Botão de fechar o modal de conexão

// Todos os inputs de dígito do código (cada caixinha = 1 dígito)
/** @type {HTMLInputElement[]} */
const inputs = Array.from(document.querySelectorAll("input.code-input-box"));

// Código do próprio dispositivo quando ele vira servidor (evita auto-conexão)
/** @type {string | null} */
let meuCodigoServidor = null;


// ─── Utilitários ─────────────────────────────────────────────────────────────

function limparInputs() {
    inputs.forEach(input => { input.value = ""; });
}

// Verifica se todos os dígitos foram preenchidos e, se sim, envia o código ao servidor
function enviarCodigoSeCompleto() {
    if (!inputs.every(input => input.value !== "")) return;

    const codigo = inputs.map(input => input.value).join("");

    // Bloqueia auto-conexão já no cliente
if (meuCodigoServidor !== null && codigo === meuCodigoServidor) {
        alert("Você não pode conectar ao próprio dispositivo servidor.");
        limparInputs();
        inputs[0].focus();
        return;
    }
    
    if (socket.readyState !== WebSocket.OPEN) {
        alert("O servidor está desconectado. Recarregue a página.");
        return;
    }

    socket.send(JSON.stringify({ type: "conectar", codigo: codigo }));
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

// "Se tornar o dispositivo servidor": solicita um código único ao servidor
document.getElementById("btn-tornar-servidor")?.addEventListener("click", () => {
    if (socket.readyState !== WebSocket.OPEN) {
        alert("Servidor desconectado. Recarregue a página.");
        return;
    }
    socket.send(JSON.stringify({ type: "tornar-servidor" }));
    telaDeCodigo?.classList.remove("active");
});

// "Conectar dispositivo": abre o modal para digitar o código
document.getElementById("btn-conectar")?.addEventListener("click", () => {
    limparInputs();
    telaDeConectar?.classList.add("active");
    inputs[0].focus();
});

fecharModalConectar?.addEventListener("click", () => {
    telaDeConectar?.classList.remove("active");
    limparInputs();
});

fecharModal?.addEventListener("click", () => {
    telaDeCodigo?.classList.remove("active");
});


// ─── Mensagens recebidas do servidor WebSocket ────────────────────────────────
socket.addEventListener("message", event => {
    let dados;
    try {
        dados = JSON.parse(event.data);
    } catch {
        console.error("Mensagem inválida do servidor:", event.data);
        return;
    }
    console.log("Mensagem recebida:", dados);

    if (dados.type === "codigo-gerado") {
        meuCodigoServidor = String(dados.codigo);
        if (codigoTexto) codigoTexto.textContent = String(dados.codigo);
        telaDeCodigo?.classList.add("active");
    }

    if (dados.type === "conectado") {
        telaDeConectar?.classList.remove("active");
        limparInputs();
        alert(dados.mensagem || "Conexão estabelecida com sucesso!");
    }

    if (dados.type === "erro") {
        alert(dados.mensagem || "Não foi possível conectar.");
        limparInputs();
        inputs[0]?.focus();
    }

    if (dados.type === "dispositivo-conectou") {
        console.log("Outro dispositivo conectou ao seu servidor.");
    }
});