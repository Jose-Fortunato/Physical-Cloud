const socket = new WebSocket('wss://physical-cloud-server.onrender.com');

const telaDeCodigo = document.getElementById("modal-codigo");
const codigoTexto = document.getElementById("codigo-texto");
const fecharModal = document.getElementById("btn-fechar-modal");
const telaDeConectar = document.getElementById("modal-conectar");
const fecharModalConectar = document.getElementById("btn-fechar-conectar");
const inserirCodigo = document.getElementById("inserir-Codigo");
const inputs = Array.from(document.querySelectorAll(".code-input-box"));

let meuCodigoServidor = null;

// Limpa os 6 campos de input
function limparInputs() {
    inputs.forEach(input => { input.value = ""; });
}

// Envia o código ao servidor quando os 6 campos estiverem preenchidos
function enviarCodigoSeCompleto() {
    if (!inputs.every(input => input.value !== "")) return;

    const codigo = inputs.map(input => input.value).join("");

    // Impede conectar ao próprio código
    if (meuCodigoServidor !== null && codigo === String(meuCodigoServidor)) {
        alert("Este é o código do seu próprio dispositivo. Digite o código de outro dispositivo.");
        limparInputs();
        inputs[0].focus();
        return;
    }

    socket.send(JSON.stringify({ type: "conectar", codigo: codigo }));
}

// Eventos dos inputs: digitar, backspace e colar
inputs.forEach((input, index) => {
    input.addEventListener("input", () => {
        input.value = input.value.replace(/\D/g, "").slice(0, 1);
        enviarCodigoSeCompleto();
        if (input.value && index < inputs.length - 1) {
            inputs[index + 1].focus();
        }
    });

    input.addEventListener("keydown", event => {
        if (event.key === "Backspace" && input.value === "" && index > 0) {
            inputs[index - 1].focus();
        }
    });

    input.addEventListener("paste", event => {
        const colado = (event.clipboardData || window.clipboardData)
            .getData("text").replace(/\D/g, "").slice(0, 6);
        if (!colado) return;
        event.preventDefault();
        limparInputs();
        colado.split("").forEach((digito, i) => { if (inputs[i]) inputs[i].value = digito; });
        enviarCodigoSeCompleto();
    });
});

// Botão "Se tornar o dispositivo servidor"
document.getElementById("btn-tornar-servidor").addEventListener("click", () => {
    socket.send(JSON.stringify({ type: "tornar-servidor" }));
    telaDeCodigo.classList.remove("active");
});

// Botão "Conectar dispositivo"
document.getElementById("btn-conectar").addEventListener("click", () => {
    limparInputs();
    telaDeConectar.classList.add("active");
    inputs[0].focus();
});

// Fechar modais
fecharModalConectar.addEventListener("click", () => {
    telaDeConectar.classList.remove("active");
    limparInputs();
});

fecharModal.addEventListener("click", () => {
    telaDeCodigo.classList.remove("active");
});

// Receber mensagens do servidor
socket.addEventListener("message", event => {
    const dados = JSON.parse(event.data);
    console.log("Mensagem recebida:", dados);

    if (dados.type === "codigo-gerado") {
        meuCodigoServidor = String(dados.codigo);
        codigoTexto.textContent = dados.codigo;
        telaDeCodigo.classList.add("active");
    }

    if (dados.type === "conectado") {
        telaDeConectar.classList.remove("active");
        limparInputs();
        alert(dados.mensagem || "Conexão estabelecida com sucesso!");
    }

    if (dados.type === "erro") {
        alert(dados.mensagem || "Não foi possível conectar.");
        limparInputs();
        inputs[0].focus();
    }

    if (dados.type === "dispositivo-conectou") {
        console.log("Outro dispositivo conectou ao seu servidor.");
    }
});