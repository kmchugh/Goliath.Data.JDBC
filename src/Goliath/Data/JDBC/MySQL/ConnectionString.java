/* =========================================================
 * JDBCConnectionString.java
 *
 * Author:      kmchugh
 * Created:     14-Dec-2007, 12:18:29
 * 
 * Description
 * --------------------------------------------------------
 * Connection string for JDBC sources
 *
 * Change Log
 * --------------------------------------------------------
 * Init.Date        Ref.            Description
 * --------------------------------------------------------
 * 
 * =======================================================*/

package Goliath.Data.JDBC.MySQL;

import Goliath.Interfaces.Data.ConnectionTypes.IDataLayerAdapter;

/**
 * Connection string for JDBC sources
 *
 * @version     1.0 14-Dec-2007
 * @author      kmchugh
**/
public class ConnectionString extends Goliath.Data.ConnectionString
{
    /** Creates a new instance of ConnectionString */
    public ConnectionString()
    {
        super();
        
        // If the character encoding is not set, set it to utf-8;
        
        if (getParameter("characterEncoding") == null)
        {
            setParameter("characterEncoding", "utf-8");
        }
    }

    @Override
    protected IDataLayerAdapter onCreateDataLayerAdapter()
    {
        return new Goliath.Data.JDBC.MySQL.DataLayerAdapter(this);
    }
}
