// PhysicalCloud-auto.js
// Módulo de Modo Automático — APENAS para o app Android (WebView)
// Funciona também em navegador (puro JS, sem dependência do bridge Android)
//
// Responsabilidades:
//   1. Gerar e persistir um ID permanente PC- com 27 caracteres aleatórios em localStorage
//   2. Calcular o hash SHA-256 do ID via crypto.subtle (nunca expõe o ID puro)
//   3. Registrar o hash no servidor quando o WebSocket abre
//   4. Auto-aceitar pedidos de conexão do tipo "pedido-automatico"
//
// Segurança: "O ID fica apenas no localStorage do dispositivo dono.
//             O servidor só vê hashes." — nunca enviar o ID em texto puro.

(function () {
    "use strict";

    const CHAVE_ID_AUTO = "physicalCloudAutoId";
    const CHAVE_AUTO_ATIVO = "physicalCloudAutoAtivo";

    // ─── Gerar / recuperar ID permanente ────────────────────────────────────

    function gerarIdAuto() {
        const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        const out = [];
        while (out.length < 27) {
            const bytes = new Uint8Array(32);
            crypto.getRandomValues(bytes);
            for (const b of bytes) {
                if (b < Math.floor(256 / chars.length) * chars.length) {
                    out.push(chars[b % chars.length]);
                    if (out.length === 27) break;
                }
            }
        }
        return "PC-" + out.join("");
    }

    function obterIdAuto() {
        try {
            let id = localStorage.getItem(CHAVE_ID_AUTO);
            if (id && /^PC-[A-Z0-9]{27}$/.test(id)) return id;
            id = gerarIdAuto();
            localStorage.setItem(CHAVE_ID_AUTO, id);
            return id;
        } catch {
            return gerarIdAuto(); // modo privado — ID não persistido
        }
    }

    function modoAutoAtivo() {
        try { return localStorage.getItem(CHAVE_AUTO_ATIVO) === "true"; } catch { return false; }
    }

    function ativarModoAuto(ativo) {
        try { localStorage.setItem(CHAVE_AUTO_ATIVO, ativo ? "true" : "false"); } catch {}
    }

    // ─── Hash SHA-256 ────────────────────────────────────────────────────────

    async function calcularHash(id) {
        const encoder = new TextEncoder();
        const data = encoder.encode(id.trim().toUpperCase());
        const hashBuffer = await crypto.subtle.digest("SHA-256", data);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, "0")).join("");
    }

    // Migra códigos legados de 8 caracteres para 27 caracteres aleatórios.
    try {
        if (modoAutoAtivo()) {
            const antigo = localStorage.getItem(CHAVE_ID_AUTO);
            if (antigo && /^PC-[A-Z0-9]{8}$/.test(antigo)) {
                localStorage.setItem("physicalCloudAutoIdLegacyToRevoke", antigo);
                localStorage.setItem(CHAVE_ID_AUTO, gerarIdAuto());
            }
        }
    } catch {}

    // ─── Registro no servidor ────────────────────────────────────────────────

    async function registrarNoServidor() {
        if (!modoAutoAtivo()) return;
        const id = obterIdAuto();
        try {
            const antigo = localStorage.getItem("physicalCloudAutoIdLegacyToRevoke");
            const api = /** @type {any} */ (window).PhysicalCloud;
            if (antigo && api && typeof api.enviar === "function") {
                api.enviar({ type: "desregistrar-id-automatico", idHash: await calcularHash(antigo) });
                localStorage.removeItem("physicalCloudAutoIdLegacyToRevoke");
            }
            const idHash = await calcularHash(id);
            if (!api || typeof api.enviar !== "function") return;
            api.enviar({ type: "registrar-id-automatico", idHash });
            console.log("[AutoMode] ID registrado no servidor (hash).");
        } catch (err) {
            console.error("[AutoMode] Erro ao registrar hash:", err);
        }
    }

    // ─── Auto-aceite de pedido de conexão ────────────────────────────────────

    function agendarAceiteAutomatico(msg) {
        setTimeout(() => {
            try {
                const api = /** @type {any} */ (window).PhysicalCloud;
                if (!api || typeof api.enviar !== "function") return;
                // Responde ao servidor aceitando a conexão com permissão total
                api.enviar({
                    type: "responder-pedido",
                    sessaoId: msg.sessaoId,
                    aceitar: true,
                    permissao: "leitura-escrita",
                });
                console.log("[AutoMode] Conexão aceita automaticamente. SessaoId:", msg.sessaoId);
            } catch (err) {
                console.error("[AutoMode] Erro no aceite automático:", err);
            }
        }, 300);
    }

    // ─── Escuta eventos do PhysicalCloud.js ─────────────────────────────────

    // Registra no servidor toda vez que o WebSocket abre (ou reconecta)
    window.addEventListener("physicalcloud:websocket-aberto", registrarNoServidor);
    if (window.PhysicalCloud?.isSocketOpen?.()) registrarNoServidor();

    // Recebe mensagens do servidor via event bus
    window.addEventListener("physicalcloud:mensagem-recebida", (event) => {
        const msg = /** @type {CustomEvent} */ (event).detail;
        if (!msg || typeof msg.type !== "string") return;

        switch (msg.type) {
            case "registrar-id-automatico-ok":
                console.log("[AutoMode] Servidor confirmou o registro.");
                break;

            case "pedido-automatico":
                // O servidor encontrou este dispositivo via "conectar-por-id"
                // e está pedindo que a conexão seja aceita automaticamente.
                if (modoAutoAtivo()) {
                    agendarAceiteAutomatico(msg);
                } else {
                    console.log("[AutoMode] pedido-automatico recebido mas modo auto está desativado.");
                }
                break;
        }
    });

    // ─── API pública ─────────────────────────────────────────────────────────

    /** @type {any} */ (window).PhysicalCloudAuto = {
        obterIdAuto,
        modoAutoAtivo,
        ativarModoAuto,
        registrarNoServidor,
    };

    console.log("[AutoMode] Módulo carregado. Auto ativo:", modoAutoAtivo());
})();
