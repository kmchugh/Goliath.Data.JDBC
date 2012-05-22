/* ========================================================
 * ConnectionString.java
 *
 * Author:      kenmchugh
 * Created:     Mar 23, 2011, 12:38:19 PM
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

import Goliath.Interfaces.Data.ConnectionTypes.IDataLayerAdapter;


        
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
public class ConnectionString extends Goliath.Data.ConnectionString
{
    @Override
    protected IDataLayerAdapter onCreateDataLayerAdapter()
    {
        return new Goliath.Data.JDBC.MSSQL.DataLayerAdapter(this);
    }
}
