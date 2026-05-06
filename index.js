const express = require('express');
const cors = require('cors');
const sql = require('mssql');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Root page with service info
app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>SQL Executor Service</title>
      <meta charset="utf-8">
      <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        h1 { color: #2563eb; }
        .status { background: #dcfce7; border: 1px solid #86efac; padding: 15px; border-radius: 8px; margin: 20px 0; }
        .status.error { background: #fee2e2; border-color: #fca5a5; }
        code { background: #f3f4f6; padding: 2px 6px; border-radius: 4px; }
        .endpoint { background: #f9fafb; padding: 10px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #2563eb; }
      </style>
    </head>
    <body>
      <h1>🚀 SQL Executor Service</h1>
      <div class="status">
        <strong>✅ Сервис работает!</strong><br>
        Время запуска: ${new Date().toLocaleString()}
      </div>
      
      <h2>Доступные эндпоинты:</h2>
      
      <div class="endpoint">
        <strong>POST /test</strong><br>
        Проверка подключения к MS SQL Server
      </div>
      
      <div class="endpoint">
        <strong>POST /execute</strong><br>
        Выполнение SQL запросов
      </div>
      
      <div class="endpoint">
        <strong>POST /check-table</strong><br>
        Проверка существования таблицы
      </div>
      
      <div class="endpoint">
        <strong>GET /health</strong><br>
        Проверка работоспособности сервиса
      </div>
      
      <p>Для использования в приложении Excel Importer укажите URL этого сервиса в настройках.</p>
    </body>
    </html>
  `);
});

// CORS preflight for all endpoints
app.options('/health', cors());
app.options('/execute', cors());
app.options('/test', cors());
app.options('/check-table', cors());

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Execute SQL query
app.post('/execute', async (req, res) => {
  const { server, database, username, password, query, options = {} } = req.body;

  if (!server || !database || !username || !password || !query) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields: server, database, username, password, query'
    });
  }

  const config = {
    server,
    database,
    user: username,
    password,
    options: {
      encrypt: options.encrypt !== false,
      trustServerCertificate: options.trustServerCertificate === true,
      ...options
    },
    connectionTimeout: options.connectionTimeout || 30000,
    requestTimeout: options.requestTimeout || 30000
  };

  let pool = null;

  try {
    pool = await sql.connect(config);
    
    const result = await pool.request().query(query);
    
    res.json({
      success: true,
      recordset: result.recordset,
      rowsAffected: result.rowsAffected,
      recordsets: result.recordsets
    });
  } catch (error) {
    console.error('SQL Error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: error.code,
      originalError: error.originalError?.message
    });
  } finally {
    if (pool) {
      await pool.close();
    }
  }
});

// Test connection
app.post('/test', async (req, res) => {
  const { server, database, username, password, options = {} } = req.body;

  if (!server || !database || !username || !password) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields: server, database, username, password'
    });
  }

  const config = {
    server,
    database,
    user: username,
    password,
    options: {
      encrypt: options.encrypt !== false,
      trustServerCertificate: options.trustServerCertificate === true,
      ...options
    },
    connectionTimeout: options.connectionTimeout || 15000
  };

  let pool = null;

  try {
    pool = await sql.connect(config);
    const result = await pool.request().query('SELECT @@VERSION as version');
    
    res.json({
      success: true,
      message: 'Connection successful',
      version: result.recordset[0]?.version
    });
  } catch (error) {
    console.error('Connection Error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: error.code
    });
  } finally {
    if (pool) {
      await pool.close();
    }
  }
});

// Check if table exists
app.post('/check-table', async (req, res) => {
  const { server, database, username, password, tableName, options = {} } = req.body;

  if (!server || !database || !username || !password || !tableName) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields'
    });
  }

  const config = {
    server,
    database,
    user: username,
    password,
    options: {
      encrypt: options.encrypt !== false,
      trustServerCertificate: options.trustServerCertificate === true,
      ...options
    },
    connectionTimeout: options.connectionTimeout || 15000
  };

  let pool = null;

  try {
    pool = await sql.connect(config);
    
    const existsResult = await pool.request()
      .input('tableName', sql.NVarChar, tableName)
      .query(`
        SELECT COUNT(*) as count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = @tableName
      `);
    
    const exists = existsResult.recordset[0]?.count > 0;
    
    if (!exists) {
      return res.json({
        success: true,
        exists: false,
        rowCount: 0,
        columns: []
      });
    }

    const countResult = await pool.request()
      .query(`SELECT COUNT(*) as count FROM [${tableName}]`);
    
    const rowCount = countResult.recordset[0]?.count || 0;

    const columnsResult = await pool.request()
      .input('tableName', sql.NVarChar, tableName)
      .query(`
        SELECT 
          COLUMN_NAME as name,
          DATA_TYPE as type,
          IS_NULLABLE as nullable,
          CHARACTER_MAXIMUM_LENGTH as maxLength
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = @tableName
        ORDER BY ORDINAL_POSITION
      `);

    res.json({
      success: true,
      exists: true,
      rowCount,
      columns: columnsResult.recordset
    });
  } catch (error) {
    console.error('Check table Error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: error.code
    });
  } finally {
    if (pool) {
      await pool.close();
    }
  }
});

app.listen(PORT, () => {
  console.log(`SQL Executor Service running on port ${PORT}`);
});
