package Goliath.Data.JDBC;

import Goliath.Applications.Application;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Wrapper class to allow dynamic loading of JDBC drivers
 * @author admin
 */
public class DynamicDriver extends Goliath.Object
    implements Driver
{
    private Driver m_oDriver;

    public DynamicDriver(String tcDriver)
    {
        try
        {
            m_oDriver = (Driver)Class.forName(tcDriver, true, (ClassLoader)Application.getInstance().getClassLoader()).newInstance();
            DriverManager.registerDriver(this);
        }
        catch (Throwable ex)
        {
            Application.getInstance().log(ex);
        }
    }

    @Override
    public int hashCode()
    {
        return m_oDriver.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return m_oDriver.equals(o);
    }

    @Override
    public boolean jdbcCompliant()
    {
        return m_oDriver.jdbcCompliant();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String string, Properties prprts) throws SQLException
    {
        return m_oDriver.getPropertyInfo(string, prprts);
    }

    @Override
    public int getMinorVersion()
    {
        return m_oDriver.getMinorVersion();
    }

    @Override
    public int getMajorVersion()
    {
        return m_oDriver.getMajorVersion();
    }

    @Override
    public Connection connect(String string, Properties prprts) throws SQLException
    {
        return m_oDriver.connect(string, prprts);
    }

    @Override
    public boolean acceptsURL(String string) throws SQLException
    {
        return m_oDriver.acceptsURL(string);
    }

    //@Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        return Logger.getLogger("DynamicDriver");
        //throw new UnsupportedOperationException("Not supported yet.");
    }


    

    
    
}
