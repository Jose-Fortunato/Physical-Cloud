import { createClient } from "jsr:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Configuração do CORS
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

const janelas = new Map<string, { count: number; reset: number }>();
const LIMITE_POR_JANELA = 25;
const JANELA_MS = 24 * 60 * 60 * 1000;

function obterIp(req: Request): string {
  const xff = req.headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0].trim();
  return req.headers.get("cf-connecting-ip") || "desconhecido";
}

function verificarRateLimit(ip: string): { permitido: boolean; restantes: number; resetEm: number } {
  const agora = Date.now();
  let j = janelas.get(ip);
  if (!j || agora >= j.reset) {
    j = { count: 0, reset: agora + JANELA_MS };
    janelas.set(ip, j);
  }
  if (j.count >= LIMITE_POR_JANELA) {
    return { permitido: false, restantes: 0, resetEm: j.reset };
  }
  j.count++;
  return { permitido: true, restantes: LIMITE_POR_JANELA - j.count, resetEm: j.reset };
}

Deno.serve(async (req) => {
  // Responde ao preflight do navegador (OPTIONS)
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ erro: "Método não permitido" }), {
      status: 405, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const ip = obterIp(req);
  const rl = verificarRateLimit(ip);
  if (!rl.permitido) {
    return new Response(JSON.stringify({
      erro: "Você atingiu o limite de 25 feedbacks por dia. Tente novamente amanhã.",
      resetEm: rl.resetEm,
    }), {
      status: 429,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json",
        "Retry-After": String(Math.ceil((rl.resetEm - Date.now()) / 1000)),
      },
    });
  }

  let body: any;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ erro: "JSON inválido" }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const rating = Number(body.rating);
  const categoria = typeof body.categoria === "string" ? body.categoria.slice(0, 40) : "";
  const mensagem = typeof body.mensagem === "string" ? body.mensagem.slice(0, 2000) : "";

  if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
    return new Response(JSON.stringify({ erro: "Nota inválida" }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
  if (!["elogio", "sugestao", "bug", "outro"].includes(categoria)) {
    return new Response(JSON.stringify({ erro: "Categoria inválida" }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const { error } = await supabase.from("feedback").insert([{
    rating, categoria, mensagem, source: "web",
  }]);

  if (error) {
    console.error("Erro ao inserir feedback:", error);
    return new Response(JSON.stringify({ erro: "Falha ao salvar feedback" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  return new Response(JSON.stringify({
    sucesso: true,
    restantes: rl.restantes,
  }), {
    status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
});