// @ts-check

// Abre a conexão WebSocket com o servidor remoto
const socket = new WebSocket('wss://physical-cloud-server.onrender.com');
socket.addEventListener("open", () => {
    setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({ type: "ping" }));
        }
    }, 240000); // 4 minutos
});

// ─── Referências aos elementos HTML ──────────────────────────────────────────
const telaDeCodigo        = document.getElementById("modal-codigo");        // Modal que mostra o código gerado
const codigoTexto         = document.getElementById("codigo-texto");         // Elemento onde o código é exibido
const fecharModal         = document.getElementById("btn-fechar-modal");     // Botão de fechar o modal do código
const telaDeConectar      = document.getElementById("modal-conectar");      // Modal para digitar o código de outro dispositivo
const fecharModalConectar = document.getElementById("btn-fechar-conectar");  // Botão de fechar o modal de conexão
const inserirCodigo       = document.getElementById("inserir-Codigo");       // Referência reservada (não usada diretamente)

// Pega todos os inputs de dígito do código (cada caixinha = 1 dígito)
// O seletor "input.code-input-box" garante que só elementos <input> sejam incluídos
/** @type {HTMLInputElement[]} */
const inputs = Array.from(document.querySelectorAll("input.code-input-box"));

// Armazena o código do próprio dispositivo quando ele vira servidor.
// Usado para impedir que o usuário conecte ao próprio código.
/** @type {string | null} */
let meuCodigoServidor = null;


// ─── Utilitários ─────────────────────────────────────────────────────────────

// Limpa todos os campos de input do código
function limparInputs() {
    inputs.forEach(input => { input.value = ""; });
}

// Verifica se todos os dígitos foram preenchidos e, se sim, envia o código ao servidor
function enviarCodigoSeCompleto() {
    if (!inputs.every(input => input.value !== "")) return; // Ainda faltam dígitos

    const codigo = inputs.map(input => input.value).join(""); // Junta todos os dígitos em uma string

    // Impede conectar ao próprio código servidor
                                        // Já recebeu o código string de quando se aperta tornar servidor
    if (meuCodigoServidor !== null && codigo === String(meuCodigoServidor)) {
        alert("Este é o código do seu próprio dispositivo. Digite o código de outro dispositivo.");
        limparInputs();
        inputs[0].focus();
        return;
    }

    // Envia a solicitação de conexão para o servidor WebSocket
    socket.send(JSON.stringify({ type: "conectar", codigo: codigo }));
}


// ─── Eventos dos inputs de código ────────────────────────────────────────────
inputs.forEach((input, index) => {

    // Ao digitar: aceita só números, avança para o próximo campo e verifica se está completo
    input.addEventListener("input", () => {
        input.value = input.value.replace(/\D/g, "").slice(0, 1); // Remove não-dígitos e limita a 1 caractere
        //envia o codigo String pro server ao terminar de colocar os 6 digitos
        enviarCodigoSeCompleto();
        if (input.value && index < inputs.length - 1) {
            inputs[index + 1].focus(); // Move o foco para o próximo input
        }
    });

    // Ao pressionar Backspace num campo vazio: volta o foco para o campo anterior
    input.addEventListener("keydown", event => {
        if (event.key === "Backspace" && input.value === "" && index > 0) {
            inputs[index - 1].focus();
        }
    });

    // Ao colar: distribui os dígitos colados entre os inputs e tenta enviar
    input.addEventListener("paste", event => {
        // Usa apenas event.clipboardData (padrão moderno); aborta se não estiver disponível
        const clipboardData = event.clipboardData;
        if (!clipboardData) return;

        const colado = clipboardData
            .getData("text").replace(/\D/g, "").slice(0, 6); // Pega só dígitos, máx 6
        if (!colado) return;

        event.preventDefault(); // Impede o comportamento padrão de colar
        limparInputs();
        colado.split("").forEach((digito, i) => { if (inputs[i]) inputs[i].value = digito; });
         //envia o codigo String pro server ao terminar de colocar os 6 digitos
        enviarCodigoSeCompleto();
    });
});


// ─── Botões principais ────────────────────────────────────────────────────────

// Botão "Se tornar o dispositivo servidor": solicita ao servidor um código único
// O ?. (optional chaining) evita erro caso o elemento não exista no HTML
document.getElementById("btn-tornar-servidor")?.addEventListener("click", () => {
    socket.send(JSON.stringify({ type: "tornar-servidor" }));
    telaDeCodigo?.classList.remove("active"); // Garante que o modal esteja fechado antes do código chegar
});

// Botão "Conectar dispositivo": abre o modal para digitar o código de outro dispositivo
document.getElementById("btn-conectar")?.addEventListener("click", () => {
    limparInputs();
    telaDeConectar?.classList.add("active"); // Exibe o modal de conexão
    inputs[0].focus(); // Coloca o foco no primeiro campo
});

// Botão fechar modal de conexão
fecharModalConectar?.addEventListener("click", () => {
    telaDeConectar?.classList.remove("active");
    limparInputs();
});

// Botão fechar modal do código gerado
fecharModal?.addEventListener("click", () => {
    telaDeCodigo?.classList.remove("active");
});


// ─── Mensagens recebidas do servidor WebSocket ────────────────────────────────
socket.addEventListener("message", event => {
    const dados = JSON.parse(event.data);
    console.log("Mensagem recebida:", dados);

    // Servidor gerou um código para este dispositivo → exibe no modal
    // Codigo gerado ao clicar no botão se tonar dispositivo servidor é sempre uma string
    if (dados.type === "codigo-gerado") {
        meuCodigoServidor = String(dados.codigo);             // Salva para evitar auto-conexão
        if (codigoTexto) codigoTexto.textContent = String(dados.codigo); // Mostra o código na tela
        telaDeCodigo?.classList.add("active");                // Abre o modal do código
    }

    // Conexão com outro dispositivo foi estabelecida com sucesso
    if (dados.type === "conectado") {
        telaDeConectar?.classList.remove("active"); // Fecha o modal de conexão
        limparInputs();
        alert(dados.mensagem || "Conexão estabelecida com sucesso!");
    }

    // Ocorreu algum erro (código inválido, auto-conexão, etc.)
    if (dados.type === "erro") {
        alert(dados.mensagem || "Não foi possível conectar.");
        limparInputs();
        inputs[0].focus(); // Volta o foco para o primeiro campo para o usuário tentar de novo
    }

    // Outro dispositivo se conectou a este dispositivo servidor
    if (dados.type === "dispositivo-conectou") {
        console.log("Outro dispositivo conectou ao seu servidor.");
        
    }
});