/* ========================================================
 * ConnectionStringFormatter.java
 *
 * Author:      kenmchugh
 * Created:     Mar 23, 2011, 12:38:27 PM
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

import Goliath.Collections.List;
import Goliath.Constants.StringFormatType;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Text.StringFormatter;


        
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
public class ConnectionStringFormatter extends Goliath.Text.StringFormatter<Goliath.Interfaces.Data.IConnectionString>
{
    /** Creates a new instance of JDBCMySQLStringFormatter */
    public ConnectionStringFormatter()
    {
    }

    @Override
    public String toString(Goliath.Interfaces.Data.IConnectionString toObject)
    {

        StringBuilder loBuilder = new StringBuilder("jdbc:sqlserver://");
        String lcPort = toObject.getParameter("port");
        String lcDatabase = toObject.getParameter("database");
        String lcInstanceName = toObject.getParameter("instanceName");

        loBuilder.append(toObject.<String>getParameter("hostname"));

        if (!Goliath.Utilities.isNullOrEmpty(lcInstanceName))
        {
            loBuilder.append("\\");
            loBuilder.append(lcInstanceName);
        }

        if (lcPort != null && !lcPort.isEmpty())
        {
            loBuilder.append(":" + lcPort);
        }
        
        boolean llAmp = false;
        for (String lcParameterName : toObject.getParameters().getPropertyKeys())
        {
            if (!lcParameterName.equalsIgnoreCase("port") &&
                    !lcParameterName.equalsIgnoreCase("database") &&
                    !lcParameterName.equalsIgnoreCase("hostname") &&
                    !lcParameterName.equalsIgnoreCase("instanceName") &&
                    toObject.getParameters().getProperty(lcParameterName) != null)
            {
                loBuilder.append(";" + lcParameterName + "=" + toObject.getParameters().getProperty(lcParameterName).toString());
            }
        }

        return loBuilder.toString();
    }
    
    @Override
    public void appendPrimitiveString(StringBuilder toBuilder, IConnectionString toObject, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatComplexProperty(StringBuilder toBuilder, String tcPropertyName, Object toValue, StringFormatter toFormatter, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatForPropertyCount(StringBuilder toBuilder, int tnIndex, int tnCount, StringFormatType toFormatType)
    {
        
    }

    @Override
    public String formatNullObject()
    {
        return "";
    }

    @Override
    protected String getEndTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    protected String getStartTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    public List<StringFormatType> supportedFormats()
    {
        return new List<StringFormatType>(new StringFormatType[]{StringFormatType.DEFAULT()});
    }


}