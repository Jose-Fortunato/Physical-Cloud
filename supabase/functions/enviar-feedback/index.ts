import { createClient } from "jsr:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

const LIMITE_POR_JANELA = 25;

function obterIp(req: Request): string {
  const xff = req.headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0].trim();
  return req.headers.get("cf-connecting-ip") || "desconhecido";
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ erro: "Método não permitido" }), {
      status: 405, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const ip = obterIp(req);
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  // Conta quantos feedbacks este IP enviou nas últimas 24 horas
  const dataLimite = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const { count, error: erroContagem } = await supabase
    .from("feedback")
    .select("*", { count: "exact", head: true })
    .eq("ip", ip)
    .gte("created_at", dataLimite);

  if (erroContagem) {
    console.error("Erro ao contar feedbacks:", erroContagem);
  }

  if (count !== null && count >= LIMITE_POR_JANELA) {
    return new Response(JSON.stringify({
      erro: "Você atingiu o limite de 25 feedbacks por dia. Tente novamente amanhã.",
    }), {
      status: 429,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
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

  // Salva o feedback (incluindo o IP)
  const { error } = await supabase.from("feedback").insert([{
    rating, categoria, mensagem, source: "web", ip
  }]);

  if (error) {
    console.error("Erro ao inserir feedback:", error);
    return new Response(JSON.stringify({ erro: "Falha ao salvar feedback" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  // Retorna quantos ainda restam hoje (baseado no que já foi salvo no banco)
  const restantes = Math.max(0, LIMITE_POR_JANELA - ((count || 0) + 1));

  return new Response(JSON.stringify({
    sucesso: true,
    restantes: restantes,
  }), {
    status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
});