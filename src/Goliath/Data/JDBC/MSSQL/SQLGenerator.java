/* ========================================================
 * SQLGenerator.java
 *
 * Author:      kenmchugh
 * Created:     Mar 23, 2011, 12:38:44 PM
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

import Goliath.Data.DataType;
import Goliath.Interfaces.Data.IDataType;
import Goliath.Interfaces.Data.IRelation;


        
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
public class SQLGenerator<T extends Goliath.Data.DataObjects.SimpleDataObject> extends Goliath.DynamicCode.SQLGenerator<T>
{
    @Override
    protected String getLeftBoundry()
    {
        return "`";
    }

    @Override
    protected String getRightBoundry()
    {
        return "`";
    }

    @Override
    protected String onGetAutoIncrementText()
    {
        return "IDENTITY(1,1)";
    }

    @Override
    protected String getUTF8String()
    {
        return "";
    }

    @Override
    protected String dataTypeToString(IDataType toType)
    {
        if (toType == DataType.BOOLEAN())
        {
            return toType.getName();
        }
        else if (toType == DataType.BINARY())
        {
            return "VARBINARY(MAX)";
        }
        else if (toType == DataType.DOUBLE())
        {
            return "FLOAT";
        }
        return super.dataTypeToString(toType);
    }

    @Override
    protected String generateRelationString(IRelation toRelation)
    {
        return ", CONSTRAINT " + wrapInBounds(toRelation.getName()) +
                " FOREIGN KEY (" + wrapInBounds(toRelation.getColumn().getName()) + ")" +
                " REFERENCES " + wrapInBounds(toRelation.getForeignTable().getName()) + "(" + wrapInBounds(toRelation.getForeignColumn().getName()) + ")" +
                " ON DELETE NO ACTION ON UPDATE NO ACTION";
    }
}