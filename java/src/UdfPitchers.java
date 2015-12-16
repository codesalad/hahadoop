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

	@SuppressWarnings("unchecked")
	@Override
	/**
	 * Input: bag containing player tuple(s)
	 * simplified input: {(playertuple), (playertuple)}
	 * 
	 * Output: player meets condition: 
	 * 	(is pitcher) and (strikeouts/games > avg strikeouts/games) 
	 * 
	 * Some players have more than 1 records
	 * (player, {(player,team,{positions},map[], avg), (player,team,{positions},map[], avg)}
	 */
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            DataBag t = (DataBag) input.get(0) ;
            Iterator<Tuple> tupleIterator = t.iterator();
            
            boolean isPitcher = false;
            float avgx = Float.MAX_VALUE;
            int strikeouts = 0;
            int games = 0;
            
            // Iterates through player tuples (some have more than 1)
            for (int i = 0; tupleIterator.hasNext(); i++) {
            	Tuple playerTuple = (Tuple) tupleIterator.next();
            	
            	if (playerTuple.size() == 0) continue;
            	if (playerTuple.get(2) == null 
        			|| playerTuple.get(3) == null 
        			|| playerTuple.get(4) == null) {
        			return false;
            	}
            	
            	DataBag positions = (DataBag) playerTuple.get(2);
            	Map<String, Object> bat = (Map<String, Object>) playerTuple.get(3);
            	avgx = (float) playerTuple.get(4);
            	
            	Iterator<Tuple> positionsIt = positions.iterator();
            	while (positionsIt.hasNext() && !isPitcher) {
            		String player_position = (String) positionsIt.next().get(0);
            		if (player_position.equals("Pitcher"))
            			isPitcher = true;
            	}
            	
            	if (bat.get("strikeouts") != null)
            		strikeouts += Integer.parseInt(bat.get("strikeouts").toString());
            	if (bat.get("games") != null)
            		games += Integer.parseInt(bat.get("games").toString());
            }
            
            float x = (float) strikeouts / games;
            if (isPitcher && x >= avgx)
            	return true;
            
        } catch (ExecException ee) {
            ee.printStackTrace(); 
        }
		return false;
    }

}
