// Importa os módulos necessários
const http = require('http');
const { Client } = require('pg');

// Configurações de conexão com o banco de dados PostgreSQL
const dbConfig = {
    host: 'localhost',
    port: 5432,
    database: 'banco_local',
    user: 'usuario_local',
    password: 'senha123'
};

// Função para conectar ao banco e buscar dados
async function getDataFromDatabase(query, params) {
    const client = new Client(dbConfig);
    await client.connect();
    try {
        const res = await client.query(query, params);
        return res.rows;
    } finally {
        await client.end();
    }
}

// Função para retornar a leitura mais recente de cada sensor na tabela nit2xli (endpoint /nit2xli/latest-readings)
async function getLatestReadings_nit2xli() {
    const query = `
    SELECT DISTINCT ON (devicename) *
    FROM nit2xli
    ORDER BY devicename, time DESC;
  `;
    return await getDataFromDatabase(query);
}

// Função para retornar a leitura mais recente da tabela k72623_lo (endpoint /k72623_lo/latest-readings)
async function getLatestReadings_k72623_lo() {
    const query = `
    SELECT DISTINCT ON (devicename) *
    FROM k72623_lo
    where devicename = 'Micropartículas Rótula do Taffarel'
	ORDER BY devicename, time DESC;
  `;
    return await getDataFromDatabase(query);
}

// Função para retornar as leituras das últimas 24 horas para o único sensor da tabela k72623_lo
async function getLast24HoursReadings_k72623_lo() {
    const query = `
    SELECT * FROM k72623_lo
    WHERE time >= NOW() - INTERVAL '24 HOURS'
  `;
    return await getDataFromDatabase(query);
}

// Função para retornar as leituras das últimas 24 horas para um dispositivo específico da tabela nit2xli
async function getLast24HoursReadings_nit2xli() {
    const query = `
    SELECT *
    FROM nit2xli
    WHERE time >= NOW() - INTERVAL '24 HOURS' AND devicename = 'Estação Cruzeiro'
  `;
    return await getDataFromDatabase(query);
}

// Configuração do servidor HTTP e definição de rotas
const server = http.createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');

    if (req.url === '/nit2xli/latest-readings' && req.method === 'GET') {
        // Endpoint para buscar a leitura mais recente de cada sensor na tabela nit2xli
        try {
            const readings = await getLatestReadings_nit2xli();
            res.writeHead(200);
            res.end(JSON.stringify(readings));
        } catch (error) {
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Erro ao buscar dados' }));
        }
    }
    else if (req.url === '/k72623_lo/latest-readings' && req.method === 'GET') {
        // Endpoint para buscar a leitura mais recente na tabela k72623_lo
        try {
            const readings = await getLatestReadings_k72623_lo();
            res.writeHead(200);
            res.end(JSON.stringify(readings));
        } catch (error) {
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Erro ao buscar dados' }));
        }
    } else if (req.url === '/k72623_lo/last-24h' && req.method === 'GET') {
        // Endpoint para buscar dados das últimas 24 horas da tabela k72623_lo
        try {
            const readings = await getLast24HoursReadings_k72623_lo();
            res.writeHead(200);
            res.end(JSON.stringify(readings));
        } catch (error) {
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Erro ao buscar dados' }));
        }
    } else if (req.url.startsWith('/nit2xli/last-24h') && req.method === 'GET') {
        // Endpoint para buscar dados das últimas 24 horas de cada dispositivo na tabela nit2xli
        try {
            const readings = await getLast24HoursReadings_nit2xli();
            res.writeHead(200);
            res.end(JSON.stringify(readings));
        } catch (error) {
            res.writeHead(500);
            res.end(JSON.stringify({ error: 'Erro ao buscar dados' }));
        }
    } else {
        // Rota não encontrada
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Rota não encontrada' }));
    }
});

// Inicia o servidor na porta 3000
server.listen(3000, () => {
    console.log('Servidor rodando em http://localhost:3000');
});
