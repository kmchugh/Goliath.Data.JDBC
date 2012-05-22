/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC.JavaDB;

import Goliath.Data.JDBC.DynamicDriver;
import Goliath.Data.JDBC.JDBCDataLayerAdapter;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Interfaces.Data.IDataBase;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import Goliath.Interfaces.IStringFormatter;
import java.io.File;

/**
 *
 * @author kenmchugh
 */
public class DataLayerAdapter extends JDBCDataLayerAdapter
{
    /** Creates a new instance of MySQLConnectionType */
    public DataLayerAdapter(IConnectionString toConnectionString)
    {
        super(toConnectionString);
        
        // Make sure the driver is loaded
        new DynamicDriver("org.apache.derby.jdbc.EmbeddedDriver");
    }

    @Override
    protected ISqlGenerator onCreateQueryGenerator()
    {
        return new SQLGenerator();
    }

    private String getDataDirectory(IConnectionString toConnectionString)
    {
        return toConnectionString.getParameter("dataDirectory");
    }

    @Override
    public boolean onCheckDBExists(IDataBase toDataBase)
    {
        String lcLocation = getDataDirectory(getConnectionString()) + getConnectionString().getParameter("database");
        File loDBFile = new File(lcLocation);
        return loDBFile.exists();
    }

    @Override
    protected boolean onAfterCreateDataBase(IDataBase toDataBase)
    {
        getConnectionString().setParameter("create", null);
        return true;
    }

    @Override
    protected boolean onBeforeCreateDataBase(IDataBase toDataBase)
    {
        getConnectionString().setParameter("create", "true");
        return true;
    }

    @Override
    protected boolean onCreateDataBase(IDataBase toDataBase)
    {
        String lcDataDirectory = ((ConnectionString)getConnectionString()).getDataDirectory();
        File loFile = new File(lcDataDirectory);
        if (!loFile.exists())
        {
            loFile.mkdir();
        }
        if (loFile.exists())
        {
            // Making a connection with the create parameter will create the database
            // To check if a db exists, we just have to connect to it
            IConnection loConnection = null;
            try
            {
                loConnection = Goliath.Data.ConnectionPool.getConnection(getConnectionString());
                return true;
            }
            catch (Throwable ex)
            {
                return false;
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch (Throwable ignore){}
                }
            }
        }
        return false;

    }


    @Override
    protected IStringFormatter<IConnectionString> onCreateFormatter()
    {
        return new ConnectionStringFormatter();
    }

    /**
     * Adds all the possible parameter to the connection type
     */
    @Override
    protected void onAddParameters()
    {
        super.onAddParameters();
        
        // Add any parameters that are possible
        
        // The directory with the data files
        this.addParameter("dataDirectory");

        /*
         * Specifies the key to use to:
         *  - encrypt a new database
         *  - Configure an existing unencrypted database for encryption
         *  - Boot an existing encrypted database
         *
         * Specify an alpanumeric string that is at least eight characters long
         */
        this.addParameter("bootPassword");
        // Creates a new database and connects to it
        this.addParameter("create");
        // Creates a database from a full backup specified
        this.addParameter("createFrom");
        
        this.addParameter("databaseName");
        
        this.addParameter("dataEncryption");
        
        this.addParameter("encryptionKey");
        
        this.addParameter("encryptionProvider");
        
        this.addParameter("encryptionAlgorithm");
        
        this.addParameter("logDevice");
        
        this.addParameter("newEncryptionKey");
        
        this.addParameter("newBootPassword");
        
        this.addParameter("restoreFrom");

        this.addParameter("rollForwardRecoveryFrom");

        this.addParameter("shutdown");
        
        this.addParameter("territory");
    }
}
