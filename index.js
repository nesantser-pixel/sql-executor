const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

function createDbConfig(connection) {
  const config = {
    server: connection.server || connection.host || 'localhost',
    database: connection.database || 'master',
    user: connection.user || connection.username || 'sa',
    password: connection.password || '',
    port: connection.port || 1433,
    options: {
      encrypt: connection.encrypt !== false,
      trustServerCertificate: connection.trustServerCertificate !== false
    }
  };
  return config;
}

async function executeQuery(connection, query, params = []) {
  const config = createDbConfig(connection);
  let pool = null;
  try {
    pool = await sql.connect(config);
    const request = pool.request();
    params.forEach((param, index) => {
      request.input(`param${index}`, param);
    });
    const result = await request.query(query);
    return {
      success: true,
      data: result.recordset || [],
      rowsAffected: result.rowsAffected ? result.rowsAffected[0] : 0
    };
  } catch (error) {
    return { success: false, error: error.message, code: error.code };
  } finally {
    if (pool) await pool.close();
  }
}

async function checkTable(connection, tableName) {
  const config = createDbConfig(connection);
  let pool = null;
  try {
    pool = await sql.connect(config);
    const checkQuery = `SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName`;
    const checkResult = await pool.request().input('tableName', sql.VarChar, tableName).query(checkQuery);
    const exists = checkResult.recordset[0].count > 0;
    
    if (!exists) return { success: true, exists: false, message: `Table '${tableName}' not found` };
    
    const countResult = await pool.request().query(`SELECT COUNT(*) as count FROM [${tableName}]`);
    const structureResult = await pool.request().input('tableName', sql.VarChar, tableName).query(`
      SELECT COLUMN_NAME as name, DATA_TYPE as type, IS_NULLABLE as isNullable, CHARACTER_MAXIMUM_LENGTH as maxLength
      FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION
    `);
    
    return {
      success: true,
      exists: true,
      rowCount: countResult.recordset[0].count,
      columns: structureResult.recordset
    };
  } catch (error) {
    return { success: false, error: error.message };
  } finally {
    if (pool) await pool.close();
  }
}

async function testConnection(connection) {
  const config = createDbConfig(connection);
  let pool = null;
  try {
    pool = await sql.connect(config);
    const result = await pool.request().query('SELECT @@VERSION as version');
    return { success: true, message: 'Connected', serverVersion: result.recordset[0].version };
  } catch (error) {
    return { success: false, error: error.message };
  } finally {
    if (pool) await pool.close();
  }
}

app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'SQL Executor', version: '1.0.0', timestamp: new Date().toISOString() });
});

app.post('/test', async (req, res) => {
  const result = await testConnection(req.body.connection);
  res.json(result);
});

app.post('/execute', async (req, res) => {
  const result = await executeQuery(req.body.connection, req.body.query, req.body.params);
  res.json(result);
});

app.post('/check-table', async (req, res) => {
  const result = await checkTable(req.body.connection, req.body.tableName);
  res.json(result);
});

app.listen(PORT, () => console.log(`SQL Executor on port ${PORT}`));
