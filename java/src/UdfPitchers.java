import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class UdfPitchers extends FilterFunc {

	@Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            Object values = input.get(0);
//            if (values instanceof DataBag)
//                return ((DataBag)values).size() == 0;
//            else if (values instanceof Map)
//                return ((Map)values).size() == 0;
//            else{
//                throw new IOException("Cannot test a " +
//                    DataType.findTypeName(values) + " for emptiness.");
//            }
            DataBag t = (DataBag) values;
            Iterator<Tuple> tupleIterator = t.iterator();
            for (int i = 0; tupleIterator.hasNext(); i++) {
            	if (i == 0) {
            		String text = (String) tupleIterator.next().get(0);
            		if (text.equals("Jarrod Saltalamacchia")) 
            			return true;
            	}
            }
        } catch (ExecException ee) {
            ee.printStackTrace(); 
        }
		return false;
    }

}
