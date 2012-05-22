/* =========================================================
 * MySQLConnectionType.java
 *
 * Author:      kmchugh
 * Created:     14-Dec-2007, 13:32:22
 * 
 * Description
 * --------------------------------------------------------
 * MySQL Connection type
 *
 * Change Log
 * --------------------------------------------------------
 * Init.Date        Ref.            Description
 * --------------------------------------------------------
 * 
 * =======================================================*/

package Goliath.Data.JDBC.MySQL;

import Goliath.Data.JDBC.DynamicDriver;
import Goliath.Data.JDBC.JDBCDataLayerAdapter;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Interfaces.Data.IDataBase;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import Goliath.Interfaces.IStringFormatter;
import java.sql.Statement;

/**
 * Used for connecting to MySQL data sources
 * using JDBC and the J/Connect driver com.mysql.jdbc.Driver
 *
 * @version     1.0 14-Dec-2007
 * @author      kmchugh
**/
public class DataLayerAdapter extends JDBCDataLayerAdapter
{
    /** Creates a new instance of MySQLConnectionType */
    public DataLayerAdapter(IConnectionString toConnectionString)
    {
        super(toConnectionString);
        
        // Make sure the driver is loaded
        new DynamicDriver("com.mysql.jdbc.Driver");
    }

    @Override
    protected IStringFormatter<IConnectionString> onCreateFormatter()
    {
        return new ConnectionStringFormatter();
    }

    @Override
    protected ISqlGenerator onCreateQueryGenerator()
    {
        return new SQLGenerator();
    }
    
    @Override
    public boolean onCheckDBExists(IDataBase toDataBase)
    {
        boolean llReturn = false;
        // TODO: Suppress the logging of the error if the db does not exist, it is okay if it doesn't, not really an error
        // To check if a db exists, we just have to connect to it
        IConnection loConnection = getConnection();

        llReturn = loConnection != null;

        if (loConnection != null)
        {
            try
            {
                loConnection.close();
            }
            catch (Throwable ignore){}
        }

        return llReturn;
    }

    @Override
    protected boolean onCreateDataBase(IDataBase toDataBase)
    {
        // We need to connect to the mysql database
        String lcDatabase = getConnectionString().getParameter("database");
        getConnectionString().setParameter("database", "mysql");

        IConnection loConnection = getConnection();
        if (loConnection != null)
        {
            try
            {
                ISqlGenerator loGenerator = getQueryGenerator();
                String lcCreate = loGenerator.generateCreateDataBase(toDataBase);
                Statement loStatement = loConnection.createStatement();
                loStatement.executeUpdate(lcCreate);
                return true;
            }
            catch (Throwable ignore)
            {
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch(Throwable ignore)
                    {}
                }
                getConnectionString().setParameter("database", lcDatabase);
            }
        }
        return false;
    }

    /**
     * Adds all the possible parameter to the connection type 
     */
    @Override
    protected void onAddParameters()
    {
        super.onAddParameters();

        // Add any parameters that are possible


        // The host to connect to
        this.addParameter("hostname");
        // The port to connect on
        this.addParameter("port");
        // The name of the class that the driver should use for creating socket connections
        this.addParameter("socketFactory");
        // Timeout for socket connections 0 being no timeout
        this.addParameter("connectTimeout");
        // Timeout on network socket operations 0 being no timeout
        this.addParameter("socketTimeout");
        /*
         * A comma-delimited list of classes that implement 
         * "com.mysql.jdbc.ConnectionLifecycleInterceptor" that should notified of connection 
         * lifecycle events (creation, destruction, commit, rollback, setCatalog and setAutoCommit) 
         * and potentially alter the execution of these commands. ConnectionLifecycleInterceptors 
         * are "stackable", more than one interceptor may be specified via the configuration 
         * property as a comma-delimited list, with the interceptors executed in order from left 
         * to right.
         */
        this.addParameter("connectionLifecycleInterceptors");
        /*
         * Load the comma-delimited list of configuration properties before parsing the URL 
         * or applying user-specified properties. These configurations are explained in the 
         * 'Configurations' of the documentation.
         */
        this.addParameter("useConfigs");
        /*
         * Set the CLIENT_INTERACTIVE flag, which tells MySQL to timeout connections based 
         * on INTERACTIVE_TIMEOUT instead of WAIT_TIMEOUT
         * 
         */
        this.addParameter("interactiveClient");
        /*
         * Hostname or IP address given to explicitly configure the interface that the driver 
         * will bind the client side of the TCP/IP connection to when connecting.
         */
        this.addParameter("localSocketAddress");
        /*
         * An implementation of com.mysql.jdbc.ConnectionPropertiesTransform that the driver 
         * will use to modify URL properties passed to the driver before attempting a connection
         */
        this.addParameter("propertiesTransform");
        /*
         * Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
         */
        this.addParameter("useCompression");
        
        /*
         * Allow the use of ';' to delimit multiple queries during one statement (true/false), 
         * defaults to 'false'
         */
        this.addParameter("allowMultiQueries");
        /*
         * Use SSL when communicating with the server (true/false), defaults to 'false'
         */
        this.addParameter("useSSL");
        /*
         * Require SSL connection if useSSL=true? (defaults to 'false').
         */
        this.addParameter("requireSSL");
        /*
         * Password for the client certificates KeyStore
         */
        this.addParameter("clientCertificateKeyStorePassword");
        /*
         * KeyStore type for client certificates (NULL or empty means use default, standard keystore 
         * types supported by the JVM are "JKS" and "PKCS12", your environment may have more available 
         * depending on what security products are installed and available to the JVM.
         */
        this.addParameter("clientCertificateKeyStoreType");
        /*
         * URL to the client certificate KeyStore (if not specified, use defaults)
         */
        this.addParameter("clientCertificateKeyStoreUrl");
        /*
         * Password for the trusted root certificates KeyStore
         */
        this.addParameter("trustCertificateKeyStorePassword");
        /* 
         * KeyStore type for trusted root certificates (NULL or empty means use default, standard 
         * keystore types supported by the JVM are "JKS" and "PKCS12", your environment may have more
         * available depending on what security products are installed and available to the JVM.
         */
        this.addParameter("trustCertificateKeyStoreType");
        /*
         * URL to the trusted root certificate KeyStore (if not specified, use defaults)
         */
        this.addParameter("trustCertificateKeyStoreUrl");
        /*
         * Take measures to prevent exposure sensitive information in error messages and clear data 
         * structures holding sensitive data when possible? (defaults to 'false')
         */         
        this.addParameter("paranoid");
         /*
          * The name of a class that implements "com.mysql.jdbc.log.Log" that will be used to 
          * log messages to. (default is "com.mysql.jdbc.log.StandardLogger", which logs to STDERR)
          */         
        this.addParameter("logger");
         /*
          * Trace queries and their execution/fetch times to the configured logger (true/false) 
          * defaults to 'false'
          */         
        this.addParameter("profileSQL");
        /*
         * Should the driver gather performance metrics, and report them via the configured logger 
         * every 'reportMetricsIntervalMillis' milliseconds?
         */
        this.addParameter("gatherPerfMetrics");
        /* 
         * If 'gatherPerfMetrics' is enabled, how often should they be logged (in ms)?
         */
        this.addParameter("reportMetricsIntervalMillis");
        
        /*
         * Should the driver dump the SQL it is executing, including server-side prepared statements to 
         * STDERR?
         */
        this.addParameter("autoGenerateTestcaseScript");
        /* 
         * Should the driver dump the contents of the query sent to the server in the
         * message for SQLExceptions?
         */
        this.addParameter("dumpQueriesOnException");
        /*
         * Should queries that take longer than 'slowQueryThresholdMillis' be logged?
         */
        this.addParameter("logSlowQueries");
        /*
         * If 'logSlowQueries' is enabled, how long should a query (in ms) before it is logged as 'slow'?
         */
        this.addParameter("slowQueryThresholdMillis");
        /*
         * A comma-separated list of name/value pairs to be sent as SET SESSION ... 
         * to the server when the driver connects.
         */
        this.addParameter("sessionVariables");
        /*
         * Creates the database given in the URL if it doesn't yet exist. Assumes the 
         * configured user has permissions to create databases.
         */
        this.addParameter("createDatabaseIfNotExist");
        /*
         * The maximum number of rows to return (0, the default means return all rows).
         */
        this.addParameter("maxRows");

        this.addParameter("characterEncoding");

        
    }
}
