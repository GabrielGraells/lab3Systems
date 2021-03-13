package upf.edu.model;

import java.util.Comparator;

public class HashtagComparator implements Comparator<HashTagCount> {
	@Override
	public int compare(HashTagCount a, HashTagCount b) {
		return (int) (a.count-b.count);
	}

}
