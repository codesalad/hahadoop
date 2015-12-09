import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class UdfPercentage extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			if (input.size() == 2) {
				if (input.get(0) == null) return null;
				if (input.get(1) == null) return null;
				int part = (int) input.get(0);
				int total = (int) input.get(1);
				double percentage = ((double) part / total) * 100;
				return new DecimalFormat("#0.00").format(percentage) + "%";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
