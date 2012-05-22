/* ========================================================
 * DataLayerAdapter.java
 *
 * Author:      kenmchugh
 * Created:     Mar 23, 2011, 12:38:36 PM
 *
 * Description
 * --------------------------------------------------------
 * General Class Description.
 *
 * Change Log
 * --------------------------------------------------------
 * Init.Date        Ref.            Description
 * --------------------------------------------------------
 *
 * ===================================================== */

package Goliath.Data.JDBC.MSSQL;

import Goliath.Data.JDBC.DynamicDriver;
import Goliath.Data.JDBC.JDBCDataLayerAdapter;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Interfaces.Data.IDataBase;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import Goliath.Interfaces.IStringFormatter;
import java.sql.Statement;


        
/**
 * Class Description.
 * For example:
 * <pre>
 *      Example usage
 * </pre>
 *
 * @see         Related Class
 * @version     1.0 Mar 23, 2011
 * @author      kenmchugh
**/
public class DataLayerAdapter extends JDBCDataLayerAdapter
{
    /** Creates a new instance of MySQLConnectionType */
    public DataLayerAdapter(IConnectionString toConnectionString)
    {
        super(toConnectionString);

        // Make sure the driver is loaded
        new DynamicDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
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
        getConnectionString().setParameter("database", "sqlserver");

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

        /**
         * String [<=128 char]
         * The SQL Server instance name to connect to. When it is not specified,
         * a connection is made to the default instance. For the case where both
         * the instanceName and port are specified, see the notes for port.
         */
        this.addParameter("instanceName");

        /**
         * [<=128 char]
         * The application name, or "Microsoft SQL Server JDBC Driver" if no name is provided.
         * Used to identify the specific application in various SQL Server profiling and logging tools.
         */
        this.addParameter("applicationName");


        /**
         * ["true"|"false"]
         * Only the value "true" is currently supported. If set to "false", an exception will occur.
         */
        this.addParameter("disableStatementPooling");

        /**
         * [0..65535]
         * The port where SQL Server is listening. If the port number is specified
         * in the connection string, no request to sqlbrowser is made. When the
         * port and instanceName are both specified, the connection is made to
         * the specified port. However, the instanceName is validated and an error
         * is thrown if it does not match the port.
         * Important: We recommend that the port number always be specified, as this is more secure than using sqlbrowser.
         */
        this.addParameter("port");


        /**
         * ["true"|"false"]
         * Set to "true" to specify that the SQL Server uses Secure Sockets
         * Layer (SSL) encryption for all the data sent between the client and
         * the server if the server has a certificate installed. The default
         * value is false.
         */
        this.addParameter("encrypt");

        /**
         * String
         * The name of the failover server used in a database mirroring
         * configuration. This property is used for an initial connection
         * failure to the principal server; after you make the initial connection,
         * this property is ignored. Must be used in conjunction with
         * databaseName property.
         * Note: The driver does not support specifying the server instance
         * port number for the failover partner instance as part of the
         * failoverPartner property in the connection string. However,
         * specifying the serverName, instanceName and portNumber properties of
         * the principal server instance and failoverPartner property of the
         * failover partner instance in the same connection string is supported.
         */
        this.addParameter("failoverPartner");

        /**
         * String
         * The host name to be used in validating the SQL Server SSL certificate.
         *
         * If the hostNameInCertificate property is unspecified or set to null,
         * the Microsoft SQL Server JDBC Driver will use the serverName property
         * value on the connection URL as the host name to validate the SQL
         * Server SSL certificate.
         * Note: This property is used in combination with the encrypt property
         * and the trustServerCertificate property. This property affects the
         * certificate validation, if and only if the encrypt property is set
         * to "true" and the trustServerCertificate is set to "false".
         */
        this.addParameter("hostNameInCertificate");

        /**
         * ["true"|"false"]
         * Set to "true" to indicate that Windows credentials will be used by
         * SQL Server to authenticate the user of the application. If "true,"
         * the JDBC driver searches the local computer credential cache for
         * credentials that have already been provided at the computer or
         * network logon. If "false," the username and password must be supplied.
         * Note: This connection property is only supported on Microsoft
         * Windows operating systems.
         */
        this.addParameter("integratedSecurity");

        /**
         * ["true"|"false"]
         * A "true" value only returns the last update count from an SQL
         * statement passed to the server, and it can be used on single SELECT,
         * INSERT, or DELETE statements to ignore additional update counts
         * caused by server triggers. Setting this property to "false" causes
         * all update counts to be returned, including those returned by server
         * triggers.
         * Note: This property only applies when it is used with the
         * executeUpdate methods. All other execute methods return all
         * results and update counts.
         * This property only affects update counts returned by server triggers.
         * It does not affect result sets or errors that result as part of
         * trigger execution.
         */
        this.addParameter("lastUpdateCount");


        /**
         * The number of milliseconds to wait before the database reports a
         * lock time-out. The default behavior is to wait indefinitely. If it
         * is specified, this value is the default for all statements on the
         * connection. Note that Statement.setQueryTimeout() can be used to set
         * the time-out for specific statements. The value can be 0, which
         * specifies no wait.
         */
        this.addParameter("lockTimeout");

        /**
         * [0..65535]
         * The number of seconds the driver should wait before timing out a 
         * failed connection. A zero value indicates that the timeout is the 
         * default system timeout, which is specified as 15 seconds by default. 
         * A non-zero value is the number of seconds the driver should wait 
         * before timing out a failed connection.
         */
        this.addParameter("loginTimeout");
        
        
        /**
         * [-1| 0 | 512..32767]
         * The network packet size used to communicate with SQL Server, 
         * specified in bytes. A value of -1 indicates using the server 
         * default packet size. A value of 0 indicates using the maximum value,
         * which is 32767. If this property is set to a value outside the 
         * acceptable range, an exception will occur.
         * Important: We do not recommend using the packetSize property 
         * when the encryption is enabled (encrypt=true). Otherwise, the 
         * driver might raise a connection error. For more information, 
         * see the setPacketSize method of the SQLServerDataSource class.
         */
        this.addParameter("packetSize");
        
        /**
         * ["full"|"adaptive"]
         * If this property is set to "adaptive", the minimum possible data is 
         * buffered when necessary. The default mode is "adaptive".
         * When this property is set to "full", the entire result set is read 
         * from the server when a statement is executed.
         * Note: After upgrading the JDBC driver from version 1.2, the default 
         * buffering behavior will be "adaptive." If your application has never
         * set the "responseBuffering" property and you want to keep the 
         * version 1.2 default behavior in your application, you must set the 
         * responseBufferring propery to "full" either in the connection 
         * properties or by using the setResponseBuffering method of the 
         * SQLServerStatement object.
         */
        this.addParameter("responseBuffering");

        /**
         * ["direct"|"cursor"]
         * If this property is set to "cursor," a database cursor is created
         * for each query created on the connection for TYPE_FORWARD_ONLY
         * and CONCUR_READ_ONLY cursors. This property is typically required
         * only if the application generates very large result sets that cannot
         * be fully contained in client memory. When this property is set to
         * "cursor," only a limited number of result set rows are retained in
         * client memory. The default behavior is that all result set rows are
         * retained in client memory. This behavior provides the fastest
         * performance when the application is processing all rows.
         */
        this.addParameter("selectMethod");
        
        /**
         * ["true"|"false"]
         * If the sendStringParametersAsUnicode property is set to "true", 
         * String parameters are sent to the server in Unicode format.
         * If the sendStringParametersAsUnicode property is set to “false", 
         * String parameters are sent to the server in non-Unicode format such 
         * as ASCII/MBCS instead of Unicode.
         * The default value for the sendStringParametersAsUnicode property is 
         * "true".
         * Note: The sendStringParametersAsUnicode property is only checked when
         * sending a parameter value with CHAR, VARCHAR, or LONGVARCHAR JDBC 
         * types. The new JDBC 4.0 national character methods, such as the 
         * setNString, setNCharacterStream, and setNClob methods of 
         * SQLServerPreparedStatement and SQLServerCallableStatement classes, 
         * always send their parameter values to the server in Unicode 
         * regardless of the setting of this property.
         * For optimal performance with the CHAR, VARCHAR, and LONGVARCHAR JDBC 
         * data types, an application should set the 
         * sendStringParametersAsUnicode property to "false" and use the 
         * setString, setCharacterStream, and setClob non-national character 
         * methods of the SQLServerPreparedStatement and 
         * SQLServerCallableStatement classes.
         * When the application sets the sendStringParametersAsUnicode property
         * to "false" and uses a non-national character method to access Unicode
         * data types on the server side (such as nchar, nvarchar and ntext), 
         * some data might be lost if the database collation does not support 
         * the characters in the String parameters passed by the non-national 
         * character method.
         * 
         * Note that an application should use the setNString, 
         * setNCharacterStream, and setNClob national character methods of the 
         * SQLServerPreparedStatement and SQLServerCallableStatement classes 
         * for the NCHAR, NVARCHAR, and LONGNVARCHAR JDBC data types.
         */
        this.addParameter("sendStringParametersAsUnicode");
        
        /**
         * ["true"|"false"]
         * This property was added in SQL Server JDBC Driver 3.0.
         * When true, java.sql.Time values will be sent to the server as SQL 
         * Server datetime values.
         * When false, java.sql.Time values will be sent to the server as 
         * SQL Server time values.
         * sendTimeAsDatetime can also be modified programmatically with 
         * SQLServerDataSource.setSendTimeAsDatetime.  The default value for 
         * this property may change in a future release.
         * For more information about how the SQL Server JDBC Driver configures 
         * java.sql.Time values before sending them to the server, see 
         * Configuring How java.sql.Time Values are Sent to the Server.
         */
        this.addParameter("sendTimeAsDatetime");
        
        /**
         * ["true"|"false"]
         * Set to "true" to specify that the Microsoft SQL Server JDBC Driver 
         * will not validate the SQL Server SSL certificate.
         * If "true", the SQL Server SSL certificate is automatically trusted 
         * when the communication layer is encrypted using SSL.
         * If "false", the Microsoft SQL Server JDBC Driver will validate the 
         * server SSL certificate. If the server certificate validation fails, 
         * the driver will raise an error and terminate the connection. The
         * default value is "false".
         * Note: This property is used in combination with the encrypt property.
         * This property only affects the server SSL certificate validation if 
         * and only if the encrypt property is set to "true".
         */
        this.addParameter("trustServerCertificate");
        
        /**
         * The path (including filename) to the certificate trustStore file. 
         * The trustStore file contains the list of certificates that the 
         * client trusts.
         * When this property is unspecified or set to null, the driver will 
         * rely on the trust manager factory's look up rules to determine 
         * which certificate store to use.
         * The default SunX509 TrustManagerFactory tries to locate the 
         * trusted material in the following search order:
         * 
         * A file specified by the "javax.net.ssl.trustStore" Java Virtual Machine (JVM) system property.
         * "<java-home>/lib/security/jssecacerts" file.
         * "<java-home>/lib/security/cacerts" file.
         * 
         * For more information, see the SUNX509 TrustManager Interface 
         * documentation on the Sun Microsystems Web site.
         * Note: This property only affects the certificate trustStore 
         * lookup, if and only if the encrypt property is set to "true" 
         * and the trustServerCertificate property is set to "false".
         */
        this.addParameter("trustStore");
        
        /**
         * The password used to check the integrity of the trustStore data.
         * If the trustStore property is set but the trustStorePassword property
         * is not set, the integrity of the trustStore is not checked.
         * When both trustStore and trustStorePassword properties are 
         * unspecified, the driver will use the JVM system properties, 
         * "javax.net.ssl.trustStore" and "javax.net.ssl.trustStorePassword". 
         * If the "javax.net.ssl.trustStorePassword" system property is not 
         * specified, the integrity of the trustStore is not checked.
         * If the trustStore property is not set but the trustStorePassword 
         * property is set, the JDBC driver will use the file specified by 
         * the "javax.net.ssl.trustStore" as a trust store and the integrity 
         * of the trust store is checked by using the specified 
         * trustStorePassword. This might be needed when the client application 
         * does not want to store the password in the JVM system property.
         * Note: The trustStorePassword property only affects the certificate 
         * trustStore lookup, if and only if the encrypt property is set to 
         * "true" and the trustServerCertificate property is set to "false".
         */
        this.addParameter("trustStorePassword");

        /**
         * [<=128 char]
         * The workstation ID. Used to identify the specific workstation in
         * various SQL Server profiling and logging tools. If none is
         * specified, the <empty string> is used.
         */
        this.addParameter("workstationID");
        
        /**
         * ["true"|"false"]
         * Set to "true" to specify that the driver returns XOPEN-compliant 
         * state codes in exceptions. The default is to return SQL 99 state 
         * codes.
         */
        this.addParameter("xopenStates");

    }
}