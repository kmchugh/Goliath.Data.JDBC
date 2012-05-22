/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC.JavaDB;

import Goliath.Applications.Application;
import Goliath.Interfaces.Data.ConnectionTypes.IDataLayerAdapter;

/**
 *
 * @author kenmchugh
 */
public class ConnectionString extends Goliath.Data.ConnectionString
{
    private static String DATADIRECTORY = "dataDirectory";
    /**
     * Sets the data directory for this connection string, this just sets the dataDirectory parameter
     * @param tcDirectory the data directory that has been set for this connection
     */
    public void setDataDirectory(String tcDirectory)
    {
        setParameter(DATADIRECTORY, tcDirectory);
    }

    /**
     * Gets the data directory
     * @return the data directory
     */
    @Goliath.Annotations.NotProperty
    public String getDataDirectory()
    {
        return getParameter(DATADIRECTORY);
    }

    /**
     * Gets the specified parameter.  If the parameter is the data directory and if it did not already exist, then this will set it to the
     * Application data directory
     * @return the value of the parameter specified
     */
    @Override
    protected <T> T onGetParameter(String tcName)
    {
        if(tcName.equalsIgnoreCase(DATADIRECTORY))
        {
            String lcString = super.onGetParameter(tcName);
            if (Goliath.Utilities.isNullOrEmpty(lcString))
            {
                lcString = Application.getInstance().getDirectory("data");
                setParameter(DATADIRECTORY, lcString);
            }
        }
        return super.<T>onGetParameter(tcName);
    }


    /**
     * Creates the Data Adapter for this type of connection string
     * @return the Data Layer Adapter
     */
    @Override
    protected IDataLayerAdapter onCreateDataLayerAdapter()
    {
        return new Goliath.Data.JDBC.JavaDB.DataLayerAdapter(this);
    }
}
