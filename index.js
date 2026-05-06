const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Port configuration
const PORT = process.env.PORT || 3000;

// In-memory request tracking for analytics
const requestStats = {
  totalRequests: 0,
  successfulRequests: 0,
  failedRequests: 0,
  lastRequestTime: null,
  recentRequests: []
};

// ==========================================
// CONFIGURATION - Adjust these for your environment
// ==========================================

// Default database configuration (can be overridden in requests)
const defaultDbConfig = {
  server: process.env.DB_SERVER || 'localhost',
  database: process.env.DB_DATABASE || 'master',
  user: process.env.DB_USER || 'sa',
  password: process.env.DB_PASSWORD || '',
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

// ==========================================
// HELPER FUNCTIONS
// ==========================================

/**
 * Create database config from connection parameters
 */
function createDbConfig(connection) {
  const config = {
    server: connection.server || connection.host || defaultDbConfig.server,
    database: connection.database || defaultDbConfig.database,
    user: connection.user || connection.username || defaultDbConfig.user,
    password: connection.password || defaultDbConfig.password,
    port: connection.port || 1433,
    options: {
      encrypt: connection.encrypt !== false,
      trustServerCertificate: connection.trustServerCertificate !== false
    }
  };

  // Handle authentication type
  if (connection.authType === 'windows') {
    config.authentication = {
      type: 'ntlm',
      options: {
        domain: connection.domain || '',
        userName: connection.user || '',
        password: connection.password || ''
      }
    };
    delete config.user;
    delete config.password;
  }

  return config;
}

/**
 * Execute SQL query
 */
async function executeQuery(connection, query, params = []) {
  const config = createDbConfig(connection);

  let pool = null;
  try {
    pool = await sql.connect(config);

    const request = pool.request();

    // Add parameters if provided
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
    console.error('SQL Error:', error);
    return {
      success: false,
      error: error.message,
      code: error.code
    };
  } finally {
    if (pool) {
      await pool.close();
    }
  }
}

/**
 * Check if table exists and get info
 */
async function checkTable(connection, tableName) {
  const config = createDbConfig(connection);

  let pool = null;
  try {
    pool = await sql.connect(config);

    // Check if table exists
    const checkQuery = `
      SELECT COUNT(*) as count
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_NAME = @tableName
    `;

    const checkResult = await pool.request()
      .input('tableName', sql.VarChar, tableName)
      .query(checkQuery);

    const exists = checkResult.recordset[0].count > 0;

    if (!exists) {
      return {
        success: true,
        exists: false,
        message: `Таблица '${tableName}' не существует`
      };
    }

    // Get row count
    const countQuery = `SELECT COUNT(*) as count FROM [${tableName}]`;
    const countResult = await pool.request().query(countQuery);
    const rowCount = countResult.recordset[0].count;

    // Get table structure
    const structureQuery = `
      SELECT
        COLUMN_NAME as name,
        DATA_TYPE as type,
        IS_NULLABLE as isNullable,
        CHARACTER_MAXIMUM_LENGTH as maxLength
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = @tableName
      ORDER BY ORDINAL_POSITION
    `;

    const structureResult = await pool.request()
      .input('tableName', sql.VarChar, tableName)
      .query(structureQuery);

    return {
      success: true,
      exists: true,
      rowCount,
      columns: structureResult.recordset
    };

  } catch (error) {
    console.error('Check table error:', error);
    return {
      success: false,
      error: error.message
    };
  } finally {
    if (pool) {
      await pool.close();
    }
  }
}

/**
 * Test database connection
 */
async function testConnection(connection) {
  const config = createDbConfig(connection);

  let pool = null;
  try {
    pool = await sql.connect(config);

    // Try a simple query
    const result = await pool.request().query('SELECT @@VERSION as version');

    return {
      success: true,
      message: 'Подключение успешно',
      serverVersion: result.recordset[0].version
    };

  } catch (error) {
    console.error('Connection test error:', error);
    return {
      success: false,
      error: error.message,
      code: error.code
    };
  } finally {
    if (pool) {
      await pool.close();
    }
  }
}

// ==========================================
// ROUTES
// ==========================================

// Health check
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'SQL Executor',
    version: '1.0.0',
    timestamp: new Date().toISOString()
  });
});

// Test database connection
app.post('/test', async (req, res) => {
  const { connection } = req.body;

  if (!connection) {
    return res.status(400).json({
      success: false,
      error: 'Connection parameters required'
    });
  }

  const result = await testConnection(connection);
  res.json(result);
});

// Execute SQL query
app.post('/execute', async (req, res) => {
  const startTime = Date.now();
  requestStats.totalRequests++;
  requestStats.lastRequestTime = new Date().toISOString();

  try {
    const { connection, query, params } = req.body;

    if (!connection || !query) {
      requestStats.failedRequests++;
      return res.status(400).json({
        success: false,
        error: 'Connection and query are required'
      });
    }

    console.log(`[${new Date().toISOString()}] Executing query:`, query.substring(0, 100) + '...');

    const result = await executeQuery(connection, query, params);

    if (result.success) {
      requestStats.successfulRequests++;
    } else {
      requestStats.failedRequests++;
    }

    // Track recent requests (keep last 100)
    requestStats.recentRequests.unshift({
      time: new Date().toISOString(),
      query: query.substring(0, 200),
      success: result.success,
      duration: Date.now() - startTime
    });
    requestStats.recentRequests = requestStats.recentRequests.slice(0, 100);

    res.json(result);

  } catch (error) {
    requestStats.failedRequests++;
    console.error('Execute error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Check table
app.post('/check-table', async (req, res) => {
  try {
    const { connection, tableName } = req.body;

    if (!connection || !tableName) {
      return res.status(400).json({
        success: false,
        error: 'Connection and tableName are required'
      });
    }

    const result = await checkTable(connection, tableName);
    res.json(result);

  } catch (error) {
    console.error('Check table route error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Stats endpoint
app.get('/stats', (req, res) => {
  res.json({
    ...requestStats,
    uptime: process.uptime()
  });
});

// Error handling
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`SQL Executor Service running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/`);
});