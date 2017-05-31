package dubstep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

public class TreeComparator implements Comparator<ArrayList<String>>,Serializable {
	
	public static int comp(String a1, String a2) {
		int len1 = a1.length();
		int len2 = a2.length();
		int cnt1 = 0;
		int cnt2 = 0;
		int flag = 0;
		int rtflag = 0;
		while (cnt1 < len1 && cnt1 < len2) {
			char a = a1.charAt(cnt1);
			char b = a2.charAt(cnt1);
			if (flag == 0) {
				if (a == '.' && b != '.') {
					return -1;
				} else if (b == '.' && a != '.') {
					return 1;
				} else if (a == '.' && b == '.') {
					flag = 1;
					if (cnt2 != 0) {
						return cnt2;
					}
					cnt1++;
					continue;
				} else if (rtflag == 0) {
					if (a > b) {
						rtflag = 1;
						cnt2 = 1;
					} else if (b > a) {
						rtflag = 1;
						cnt2 = -1;
					}
				}
			}

			else if (flag == 1) {
				if (a < b)
					return -1;
				else if (a > b)
					return 1;
			}
			cnt1++;

		}
		if (len1 > len2) {
			return 1;
		} else if (len1 < len2)
			return -1;
		else {
			return cnt2;
		}
	}
	
	@Override
	public int compare(ArrayList<String> arg0, ArrayList<String> arg1) {
		for (int i = 0; i < arg0.size(); i++) {
			if (!arg0.get(i).contains("a") && !arg1.get(i).contains("a")
					&& (arg1.get(i).contains(".")
							|| arg0.get(i).contains("."))) {
				int cmp;
				if (arg1.get(i).contains("e") || arg0.get(i).contains("e")) {
					Double db0 = Double.parseDouble(arg0.get(i));
					Double db1 = Double.parseDouble(arg1.get(i));
					cmp = db0.compareTo(db1);
					// return cmp;

				} else
					cmp = comp(arg0.get(i), arg1.get(i));

				if (cmp != 0) {

					return cmp;
				}

			} else {
				if (arg0.get(i).length() < arg1.get(i).length()) {
					return -1;
				} else if (arg0.get(i).length() > arg1.get(i).length()) {
					return 1;
				} else {
					int res = arg0.get(i).compareTo(arg1.get(i));
					if (res != 0) {
						return res;
					}
				}
			}
		}

		return 0;
	}
	
}