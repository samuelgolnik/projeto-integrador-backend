const { Client } = require('pg');
let lastUpdateTime;

// Conectar ao banco de dados remoto
const remoteClient = new Client({
    //dentro-10.41.20.15
    //fora-200.17.86.20
    host: '200.17.86.20',
    port: 55432,
    database: 'santa_rosa',
    user: 'piinfra',
    password: 'FkBMK4787WsA'
});

// Conectar ao banco de dados local
const localClient = new Client({
    host: 'localhost',
    port: 5432,
    database: 'banco_local',
    user: 'usuario_local',
    password: 'senha123'
});

const createTable_k72623_lo = `
    CREATE TABLE k72623_lo (
        deviceName varchar(100),
        noise float4,
        temperature float4,
        voltage float4,
        humidity float4,
        pm2_5 float4,
        time timestamptz
    )
`;

const getValues_k72623_lo = `
    SELECT
        "deviceName",
        noise,
        temperature,
        voltage,
        humidity,
        pm2_5,
        time
    FROM
        k72623_lo
`;

const createTable_nit2xli = `
    CREATE TABLE nit2xli (
        deviceName varchar(100),
        emw_rain_lvl float4,
        emw_avg_wind_speed int4,
        emw_gust_wind_speed int4,
        emw_wind_direction int4,
        emw_temperature float4,
        emw_humidity float4,
        emw_luminosity int8,
        emw_uv float4,
        emw_solar_radiation float4,
        emw_atm_pres float4,
        internal_temperature float4,
        internal_humidity float4,
        time timestamptz
    )
`;

const getValues_nit2xli = `
    SELECT
        "deviceName",
        emw_rain_lvl,
        emw_avg_wind_speed,
        emw_gust_wind_speed,
        emw_wind_direction,
        emw_temperature,
        emw_humidity,
        emw_luminosity,
        emw_uv,
        emw_solar_radiation,
        emw_atm_pres,
        internal_temperature,
        internal_humidity,
        time
    FROM
        nit2xli
`;

const updateValues_k72623_lo = `
SELECT
    "deviceName",
    noise,
    temperature,
    voltage,
    humidity,
    pm2_5,
    time
FROM
    k72623_lo
WHERE
    "time" > $1
`;

const updateValues_nit2xli = `
SELECT
    "deviceName",
    emw_rain_lvl,
    emw_avg_wind_speed,
    emw_gust_wind_speed,
    emw_wind_direction,
    emw_temperature,
    emw_humidity,
    emw_luminosity,
    emw_uv,
    emw_solar_radiation,
    emw_atm_pres,
    internal_temperature,
    internal_humidity,
    time
FROM
    nit2xli
WHERE
    "time" > $1
`;


// Obter os dados da tabela do banco remoto
async function buscarDados(query, variables = []) {
    const remoteData = await remoteClient.query(query, variables);
    const rows = remoteData.rows;
    return rows;
}

// Criar a tabela nit2xlio banco local
async function createTable(tableName, creationQuery) {
    await localClient.query(`DROP TABLE IF EXISTS ${tableName}`);
    await localClient.query(creationQuery);
}


// Inserir dados na tabela local k72623_lo
async function insertDataIntoLocalTable_k72623_lo(rows) {
    for (let row of rows) {
        const query = `
            INSERT INTO k72623_lo (deviceName, noise, temperature, voltage, humidity, pm2_5, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        `;

        const values = [
            row.deviceName,
            row.noise,
            row.temperature,
            row.voltage,
            row.humidity,
            row.pm2_5,
            row.time
        ];

        try {
            await localClient.query(query, values);
        } catch (err) {
            console.error('Erro ao inserir dados na tabela local:', err);
        }
    }
}

// Inserir dados na tabela local nit2xli
async function insertDataIntoLocalTable_nit2xli(rows) {
    for (let row of rows) {
        const query = `
            INSERT INTO nit2xli (deviceName, emw_rain_lvl, emw_avg_wind_speed, emw_gust_wind_speed, emw_wind_direction, emw_temperature, emw_humidity, emw_luminosity, emw_uv, emw_solar_radiation, emw_atm_pres, internal_temperature, internal_humidity, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `;

        const values = [
            row.deviceName,
            row.emw_rain_lvl,
            row.emw_avg_wind_speed,
            row.emw_gust_wind_speed,
            row.emw_wind_direction,
            row.emw_temperature,
            row.emw_humidity,
            row.emw_luminosity,
            row.emw_uv,
            row.emw_solar_radiation,
            row.emw_atm_pres,
            row.internal_temperature,
            row.internal_humidity,
            row.time,
        ];

        try {
            await localClient.query(query, values);
        } catch (err) {
            console.error('Erro ao inserir dados na tabela local:', err);
        }
    }
}

(async () => {
    try {
        // Conectar ao banco remoto e local
        await remoteClient.connect();
        console.log("Conectado com banco externo")
        await localClient.connect();
        console.log("Conectado com banco local")

        // Criar a tabela local
        // await createTable('k72623_lo', createTable_k72623_lo);
        // console.log("Criada tabela k72623_lo")
        // await createTable('nit2xli', createTable_nit2xli);
        // console.log("Criada tabela nit2xli")

        // Buscar os dados do banco remoto
        const rows_k72623_lo = await buscarDados(getValues_k72623_lo);
        console.log("Dados tabela k72623_lo buscados")
        const rows_nit2xli = await buscarDados(getValues_nit2xli);
        console.log("Dados tabela nit2xli buscados")

        // Inserir dados no banco local
        await insertDataIntoLocalTable_k72623_lo(rows_k72623_lo);
        console.log("Dados tabela k72623_lo inseridos")
        await insertDataIntoLocalTable_nit2xli(rows_nit2xli);
        console.log("Dados tabela nit2xli inseridos")

        lastUpdateTime = Date.now()
        lastUpdateTime = new Date(lastUpdateTime)
        lastUpdateTime = lastUpdateTime.toISOString();
        console.log(`Último update: ${lastUpdateTime}`)


    } catch (err) {
        console.error('Erro ao conectar aos bancos de dados:', err);
    }
})();

setInterval(async () => {


    try {
        // Buscar os dados do banco remoto
        const rows_k72623_lo = await buscarDados(updateValues_k72623_lo, [lastUpdateTime]);
        console.log("Dados tabela k72623_lo buscados")
        const rows_nit2xli = await buscarDados(updateValues_nit2xli, [lastUpdateTime]);
        console.log("Dados tabela nit2xli buscados")

        console.log("Novos dados tabela k72623_lo:\n" + rows_k72623_lo)
        console.log("Novos dados tabela nit2xli:\n" + rows_nit2xli)

        // Inserir dados no banco local
        await insertDataIntoLocalTable_k72623_lo(rows_k72623_lo);
        console.log("Dados tabela k72623_lo inseridos")
        await insertDataIntoLocalTable_nit2xli(rows_nit2xli);
        console.log("Dados tabela nit2xli inseridos")

        lastUpdateTime = Date.now()
        lastUpdateTime = new Date(lastUpdateTime)
        lastUpdateTime = lastUpdateTime.toISOString();
        console.log(`Último update: ${lastUpdateTime}`)
    } catch (err) {
        console.log(err)
    }
}, 300000)