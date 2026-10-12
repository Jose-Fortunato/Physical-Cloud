const socket = new WebSocket('wss://physical-cloud-server.onrender.com');
const telaDeCodigo = document.getElementById("modal-codigo");
const codigoTexto = document.getElementById("codigo-texto");
const fecharModal = document.getElementById("btn-fechar-modal");
const telaDeConectar = document.getElementById("modal-conectar");
const fecharModalConectar = document.getElementById("btn-fechar-conectar");
const inserirCodigo = document.getElementById("inserir-Codigo");
const inputs = document.querySelectorAll(".code-input-box");

inputs.forEach((input, index) => {
    input.addEventListener("input", () => {
        
        if ( Array.from(inputs).every(input => input.value !== "") ) {
         
                
const codigoFinal = Number(Array.from(inputs).map(input => input.value).join(''));

            socket.send(JSON.stringify({ type: 'conectar', codigo: codigoFinal }));
            
            console.log(codigoFinal);

        }
      
        if (input.value.length >= input.maxLength) {
            const nextInput = inputs[index + 1];
            if (nextInput) {
                nextInput.focus();
            }
        }
    });
});

inputs.forEach((input, index) => {
    input.addEventListener("keydown", (event) => {
   
            if (event.key === "Backspace" && input.value.length === 0) {
                const previousInput = inputs[index - 1];
                if (previousInput) {
                    previousInput.focus();
                }
        }
            
        
   
});
});
document.getElementById("btn-tornar-servidor").addEventListener("click", () => {
    socket.send(JSON.stringify({ type: 'tornar-servidor'}));
    telaDeCodigo.classList.remove("active");
})
document.getElementById("btn-conectar").addEventListener("click", () => {
    document.getElementsByClassName("code-input-box")[0].value = "";
     document.getElementsByClassName("code-input-box")[1].value = "";
      document.getElementsByClassName("code-input-box")[2].value = "";
       document.getElementsByClassName("code-input-box")[3].value = "";
        document.getElementsByClassName("code-input-box")[4].value = "";
         document.getElementsByClassName("code-input-box")[5].value = "";
    telaDeConectar.classList.add("active");
    console.log("Conectar button clicked");
inserirCodigo.classList.add("code-input-container");
fecharModalConectar.classList.add("modal-close");

    fecharModalConectar.addEventListener("click", () => {
    telaDeConectar.classList.remove("active");

});
    });

socket.addEventListener("message", (event) => {
    const dados = JSON.parse(event.data);
    console.log("Mensagem recebida do servidor:", dados);
    if (dados.type === "codigo-gerado") {
    telaDeCodigo.classList.add("active");
    codigoTexto.textContent = dados.codigo;
    codigoTexto.classList.add("codigo-display");
    fecharModal.classList.add("modal-close");
    fecharModal.addEventListener("click", () => {
        telaDeCodigo.classList.remove("active");
    });
}
 if (dados.type === "erro"){
    alert ("Esse não é o código certo.")
 }
if (dados.type === "conectado") {
    telaDeConectar.classList.remove("active")
}
});
