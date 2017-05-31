package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class Main {

	static int checkf = 0;
	static String line = "";
	static String mem = "";
	static int cmpfl = 0;
	static int count = -1;
	static HashMap<String, PrimitiveValue[]> rowmap;
	static HashMap<String, String[]> rowmapo;
	static PrimitiveValue[] plinevals;
	static String[] linevals = null;
	static String tname = null;
	static int qflag = 0;
	static long TIME;
	static ArrayList<String> dtypes = null;
	static ArrayList<String> alldt = new ArrayList();
	static int NUMROWS = 0;

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

	public static int compstr(PrimitiveValue arg0, PrimitiveValue arg1) {

		String datatype = dtypes.get(0);
		int cmp = 0;
		try {
			// int cmp = 0;
			if (datatype.equals("int")) {

				cmp = Long.compare(arg0.toLong(), arg1.toLong());

			} else if (datatype.equals("string") || datatype.equals("varchar") || datatype.equals("char")) {
				cmp = arg0.toString().compareTo(arg1.toString());
			} else if (datatype.equals("decimal")) {
				cmp = Double.compare(arg0.toDouble(), arg1.toDouble());
			} else if (datatype.equals("date")) {
				cmp = ((DateValue) arg0).getValue().compareTo(((DateValue) arg1).getValue());
			}
		} catch (Exception e) {
		}
		return cmp;

	}

	public static int compstr(PrimitiveValue arg0, PrimitiveValue arg1, int i) {

		String datatype = alldt.get(i);
		int cmp = 0;
		try {
			// int cmp = 0;
			if (datatype.equals("int")) {

				cmp = Long.compare(arg0.toLong(), arg1.toLong());

			} else if (datatype.equals("string") || datatype.equals("varchar") || datatype.equals("char")) {
				cmp = arg0.toString().compareTo(arg1.toString());
			} else if (datatype.equals("decimal")) {
				cmp = Double.compare(arg0.toDouble(), arg1.toDouble());
			} else if (datatype.equals("date")) {
				cmp = ((DateValue) arg0).getValue().compareTo(((DateValue) arg1).getValue());
			}
		} catch (Exception e) {
		}
		return cmp;

	}

	public static int compstr(String arg0, String arg1) {

		if (arg1.contains("date")) {
			arg1 = arg1.substring(6, arg1.length() - 2);
		}

		else if (arg1.charAt(0) == '\'') {
			arg1 = arg1.substring(1, arg1.length() - 1);
		}
		if (arg1.contains(".") || arg0.contains(".")) {

			if (arg1.contains("e") || arg0.contains("e")) {
				Double db0 = Double.parseDouble(arg0);
				Double db1 = Double.parseDouble(arg1);
				int cmp = db0.compareTo(db1);
				return cmp;

			}

			else {
				return comp(arg0, arg1);
			}

		}
		if (arg0.length() < arg1.length()) {

			return -1;
		} else if (arg0.length() > arg1.length()) {
			return 1;
		} else {
			int res = arg0.compareTo(arg1);

			return res;

		}

	}

	public static void main(String[] args) throws Exception {
		String pre = args[1];
		// System.out.println("----------- "+pre+" ------------");
		if (pre.equals("--in-mem")) {
			ArrayList<Integer> pk_seq = new ArrayList<Integer>();
			List<String> prim = new ArrayList<String>();
			List<String> l_ind;
			ArrayList<PrimitiveValue[]> allvals = new ArrayList();

			Map<String, IndexData> indexes = new HashMap<String, IndexData>();
			Map<String, LinkedHashMap<String, ColData>> schema = new HashMap<String, LinkedHashMap<String, ColData>>();
			Map<String, Info> infomap = new HashMap<String, Info>();

			Map<String, TreeMap<ArrayList<PrimitiveValue>, PrimitiveValue[]>> datamap = new HashMap();
			Map<String, HashMap<String, TreeMap<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>>>> indexmap = new HashMap();

			TreeMap<ArrayList<PrimitiveValue>, PrimitiveValue[]> data = new TreeMap();
			HashMap<String, TreeMap<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>>> index_data = new HashMap();

			while (true) {
				qflag = 0;
				System.out.print("$> ");
				if (infomap.containsKey("lineitem")) {
					infomap.get("lineitem").setmax(null);
					infomap.get("lineitem").setmin(null);
				}
				if (infomap.containsKey("orders")) {
					infomap.get("orders").setmax(null);
					infomap.get("orders").setmin(null);
				}
				String input = "";
				Scanner sin = new Scanner(System.in);
				while (true) {
					String iinput = sin.nextLine();
					input += " " + iinput;
					if (iinput.charAt(iinput.length() - 1) == ';')
						break;
				}
				TIME = System.currentTimeMillis();
				int c = 0;
				char[] str = input.toCharArray();
				for (int i = 0; i < str.length; i++) {
					if (str[i] == '\'') {
						c += 1;

					}

					if (str[i] >= 'A' && str[i] <= 'Z' && c % 2 == 0)
						str[i] += 32;

				}

				input = String.valueOf(str);
				// System.out.println(input);
				StringReader abc = new StringReader(input);
				CCJSqlParser parser = new CCJSqlParser(abc);
				Stack<PlainSelect> qstack = new Stack<PlainSelect>();
				Statement query = parser.Statement();
				List<OrderByElement> ord = null;
				Limit ltr = null;
				String whereexp = ""; // only for one table

				if (query instanceof Select) {
					int skcode = 0;
					int rcflag = 0;
					Select sel = (Select) query;
					PlainSelect ps = (PlainSelect) sel.getSelectBody();
					FromItem fromItem = ps.getFromItem();
					// System.out.println(ps.getJoins());

					if (ps.getWhere() != null)
						whereexp = ps.getWhere().toString();
					// System.out.println(whereexp);
					if (fromItem instanceof SubSelect) {
						if (ps.getOrderByElements() != null) {
							ord = ps.getOrderByElements();

						}
						if (ps.getLimit() != null)
							ltr = ps.getLimit();

						ps.setLimit(null);
						ps.setOrderByElements(null);
					}
					ps.setWhere(null);

					qstack.push(ps);

					while (fromItem instanceof SubSelect) {

						SubSelect Sub = (SubSelect) fromItem;
						ps = (PlainSelect) Sub.getSelectBody();
						if (ps.getWhere() != null) {
							if (!whereexp.equals(""))
								whereexp += " AND " + ps.getWhere().toString();
							else
								whereexp = ps.getWhere().toString();
						}
						if (ps.getOrderByElements() != null) {
							ord = ps.getOrderByElements();

						}
						if (ps.getLimit() != null)
							ltr = ps.getLimit();
						ps.setWhere(null);
						ps.setLimit(null);
						ps.setOrderByElements(null);
						qstack.push(ps);

						fromItem = ps.getFromItem();

					}
					String newq;
					PlainSelect temp;
					String temptname = "";
					if (qstack.size() > 1) {
						newq = qstack.get(qstack.size() - 2).toString();

					} else {
						newq = qstack.get(0).toString();
						temp = qstack.get(0);
						temptname = temp.getFromItem().toString();
					}
					// System.out.println(newq);
					if (qstack.size() == 1 && !whereexp.equals("")) {
						int ind = newq.lastIndexOf("FROM " + temptname) + 5 + temptname.length();
						// System.out.println("ind "+ind);
						newq = new StringBuilder(newq).insert(ind, " where " + whereexp + " ").toString();
					} else if (!whereexp.equals(""))
						newq += " WHERE " + whereexp;
					// System.out.println(newq);

					abc = new StringReader(newq);
					parser = new CCJSqlParser(abc);
					Statement newquery = parser.Statement();
					Select newsel = (Select) newquery;
					PlainSelect newps = (PlainSelect) newsel.getSelectBody();

					if (qstack.size() > 1) {
						// System.out.println(newps.getWhere());
						qstack.get(qstack.size() - 2).setWhere(newps.getWhere());
						qstack.get(qstack.size() - 2).setOrderByElements(ord);
						qstack.get(qstack.size() - 2).setLimit(ltr);

					} else {
						// System.out.println(newps.getWhere());
						qstack.get(0).setWhere(newps.getWhere());
					}

					HashMap<ArrayList<PrimitiveValue>, Integer> pkkeys = new HashMap();
					ArrayList<PrimitiveValue[]> pkstore = new ArrayList();
					ArrayList<PrimitiveValue[]> backpk = new ArrayList();
					List<SelectItem> prevselectitem = null;
					String prevtname = null;
					ArrayList<Integer> compare1 = new ArrayList<Integer>();

					int resflag = 0;
					while (!qstack.isEmpty()) {
						int skipcode = 0;

						if (qflag != 0) {

							pkstore = (ArrayList<PrimitiveValue[]>) backpk.clone();
							backpk.clear();
						}

						// System.out.println(qstack);
						ps = qstack.pop();

						Table tablename = null;
						fromItem = ps.getFromItem();
						List<SelectItem> selectitem = ps.getSelectItems();

						Expression wexp = ps.getWhere();
						// System.out.println("where 1 "+wexp+" "+selectitem);
						List<Column> group = ps.getGroupByColumnReferences();
						List<OrderByElement> order = ps.getOrderByElements();

						Limit lt = ps.getLimit();

						if (qflag == 0) {

							tablename = (Table) fromItem;
							tname = tablename.getName();
							if (selectitem.get(0).toString().equals("*")) {
								String qu = ps.toString();
								Iterator it = schema.get(tname).keySet().iterator();
								String rep = "";
								while (it.hasNext()) {
									rep += it.next() + ",";
								}
								rep = rep.substring(0, rep.length() - 1);
								qu = qu.replace("*", rep);
								// System.out.println(qu);
								StringReader abc1 = new StringReader(qu);
								CCJSqlParser parser1 = new CCJSqlParser(abc1);
								Statement query1 = parser1.Statement();
								Select sel1 = (Select) query1;
								PlainSelect ps1 = (PlainSelect) sel1.getSelectBody();
								selectitem = ps1.getSelectItems();
								if (wexp == null) {

									skipcode = 1;
								}
							}
							// System.out.println(selectitem);

						} else {
							LinkedHashMap<String, ColData> ncolmap = new LinkedHashMap<String, ColData>();
							tname = fromItem.getAlias();
							if (selectitem.get(0).toString().equals("*")) {
								String qu = ps.toString();

								Iterator it = schema.get(prevtname).keySet().iterator();
								String rep = "";
								while (it.hasNext()) {
									rep += tname + "." + it.next().toString().split("\\.")[1] + ",";
								}
								rep = rep.substring(0, rep.length() - 1);
								qu = qu.replace("*", rep);
								StringReader abc1 = new StringReader(qu);
								CCJSqlParser parser1 = new CCJSqlParser(abc1);
								Statement query1 = parser1.Statement();
								Select sel1 = (Select) query1;
								PlainSelect ps1 = (PlainSelect) sel1.getSelectBody();
								selectitem = ps1.getSelectItems();
							}

							int nsel = 0;
							Iterator<SelectItem> selt = prevselectitem.iterator();
							while (selt.hasNext()) {

								int alflag = 0;
								SelectExpressionItem colname = (SelectExpressionItem) selt.next();
								String al = colname.getAlias();
								if (al == null) {
									al = colname.toString();

									alflag = 1;
								}
								ColData newcd;
								if ((schema.get(prevtname)).containsKey(al)) {
									ColData cd = (schema.get(prevtname)).get(al);
									String dtype = cd.datatype;
									newcd = new ColData(nsel, dtype);

								} else {
									newcd = new ColData(nsel, "decimal");

								}
								if (alflag == 1) {
									String spl[] = al.split("\\.");
									al = spl[1];
								}

								ncolmap.put(tname + "." + al, newcd);
								nsel++;

							}

							schema.put(tname, ncolmap);

						}

						prevselectitem = new ArrayList<SelectItem>(selectitem);

						prevtname = tname;
						if (skipcode == 1) {
							backpk = new ArrayList<PrimitiveValue[]>(data.values());
							skcode = 1;
							qflag = 1;
							continue;
						}

						int mincount = input.toLowerCase().split("min\\(", -1).length - 1;
						Eval eval = new Eval() {

							@Override
							public PrimitiveValue eval(Column arg0) throws SQLException {
								if (checkf == 1) {
									tname = arg0.getTable().getName();
									LinkedHashMap colmap = schema.get(tname);

									ColData colget = (ColData) colmap.get(arg0.toString());
									count = colget.seq;
									return rowmap.get(tname)[count];

								}

								LinkedHashMap colmap = schema.get(tname);

								ColData colget = (ColData) colmap.get(arg0.toString());
								count = colget.seq;

								return plinevals[count];

							}
						};

						Iterator<SelectItem> it = selectitem.iterator();
						int flag = 0;
						compare1 = new ArrayList<Integer>();
						while (it.hasNext()) {
							SelectItem selectFrom = it.next();
							Expression e = ((SelectExpressionItem) selectFrom).getExpression();
							Function f = null;
							try {
								f = (Function) e;
							} catch (Exception exc) {
								compare1.add(0);
								continue;
							}

							String s = f.getName();
							if (s.equals("sum")) {
								compare1.add(1);
								flag = 1;
							} else if (s.equals("min")) {
								compare1.add(2);
								flag = 1;
							} else if (s.equals("max")) {
								compare1.add(3);
								flag = 1;
							} else if (s.equals("count")) {
								compare1.add(4);
								flag = 1;
							} else if (s.equals("avg")) {
								compare1.add(5);
								flag = 1;
							}

						}
						HashMap<String, Integer> empmap = new HashMap();
						int whrflag = 0;
						int empflag = 0;
						int whereval = 0;

						HashMap<String, HashMap<ArrayList<PrimitiveValue>, Integer>> bkmap = new HashMap();
						HashMap<ArrayList<PrimitiveValue>, Integer> bkstore = new HashMap();
						// ArrayList prm = new ArrayList<PrimitiveValue>();
						PrimitiveValue value;
						String col = null;
						Expression whr = ps.getWhere();

						// String wheres[] = wexp.toString().split("AND");
						int pkflag = 0;
						ArrayList<BinaryExpression> jlist = new ArrayList();
						Map<String, ArrayList<BinaryExpression>> whmap = new HashMap();
						Map<String, ArrayList<PrimitiveValue[]>> pkmap = new HashMap();
						ArrayList<BinaryExpression> evlist = new ArrayList();
						// System.out.println(wexp + " " + qflag);
						if (wexp != null && qflag == 0) {

							ArrayList<Expression> qwe = new ArrayList();
							if (whr instanceof AndExpression) {
								while (whr instanceof AndExpression) {
									qwe.add(((AndExpression) whr).getRightExpression());

									whr = ((AndExpression) whr).getLeftExpression();
								}
								qwe.add(whr);
							}

							else if (whr instanceof OrExpression) {
								OrExpression asd = new OrExpression();
								Expression e = (AndExpression) ((OrExpression) whr).getRightExpression();
								while (e instanceof AndExpression) {
									qwe.add(((AndExpression) e).getRightExpression());
									e = ((AndExpression) e).getLeftExpression();

								}
								asd.setRightExpression(e);
								e = (AndExpression) ((OrExpression) whr).getLeftExpression();
								asd.setLeftExpression(((AndExpression) e).getRightExpression());
								e = ((AndExpression) e).getLeftExpression();
								while (e instanceof AndExpression) {
									qwe.add(((AndExpression) e).getRightExpression());
									e = ((AndExpression) e).getLeftExpression();

								}
								qwe.add(e);
								evlist.add(asd);
								whmap.put("lineitem", evlist);
							} else {
								qwe.add(whr);
							}

							// System.out.println(qwe);
							PrimitiveValue min = null;
							PrimitiveValue max = null;
							int remi = 0;
							int mini = 0, maxi = 0;
							for (Expression m : qwe) {

								BinaryExpression k = (BinaryExpression) m;

								// System.out.println(k);
								if (k.getLeftExpression() instanceof Column && k.getRightExpression() instanceof Column
										&& !(((Column) k.getLeftExpression()).getTable().getName()
												.equals(((Column) k.getLeftExpression()).getTable().getName()))) {
									// jlist.add(k);
									continue;
								}

								if (k.getLeftExpression() instanceof Column) {
									// System.out.println(((Column)k.getLeftExpression()).getTable().getName());
									if (k.getLeftExpression().toString().equals("lineitem.receiptdate")) {
										if (k instanceof GreaterThanEquals) {
											min = new DateValue(k.getRightExpression().toString().substring(6,
													k.getRightExpression().toString().length() - 2));
											infomap.get("lineitem").setmin(min);
											mini = remi;
										} else if (k instanceof GreaterThan) {
											min = new DateValue(LocalDate
													.parse(k.getRightExpression().toString().substring(6,
															k.getRightExpression().toString().length() - 2))
													.plusDays(1).toString());
											infomap.get("lineitem").setmin(min);
											mini = remi;
										} else if (k instanceof MinorThan) {
											max = new DateValue(LocalDate
													.parse(k.getRightExpression().toString().substring(6,
															k.getRightExpression().toString().length() - 2))
													.minusDays(1).toString());
											infomap.get("lineitem").setmax(max);
											maxi = remi;
										} else if (k instanceof MinorThanEquals) {
											max = new DateValue(k.getRightExpression().toString().substring(6,
													k.getRightExpression().toString().length() - 2));
											infomap.get("lineitem").setmax(max);
											maxi = remi;
										}

									}

									else if (k.getLeftExpression().toString().equals("orders.orderdate")) {
										if (k instanceof GreaterThanEquals) {
											min = new DateValue(k.getRightExpression().toString().substring(6,
													k.getRightExpression().toString().length() - 2));
											infomap.get("orders").setmin(min);
											mini = remi;

										} else if (k instanceof GreaterThan) {
											min = new DateValue(LocalDate
													.parse(k.getRightExpression().toString().substring(6,
															k.getRightExpression().toString().length() - 2))
													.plusDays(1).toString());
											infomap.get("orders").setmin(min);
											mini = remi;
										} else if (k instanceof MinorThan) {
											max = new DateValue(LocalDate
													.parse(k.getRightExpression().toString().substring(6,
															k.getRightExpression().toString().length() - 2))
													.minusDays(1).toString());
											infomap.get("orders").setmax(max);
											maxi = remi;
										} else if (k instanceof MinorThanEquals) {
											max = new DateValue(k.getRightExpression().toString().substring(6,
													k.getRightExpression().toString().length() - 2));
											infomap.get("orders").setmax(max);
											maxi = remi;
										}

									}

								} else {
									if (k.getRightExpression().toString().equals("lineitem.receiptdate")) {
										if (k instanceof GreaterThanEquals) {
											min = new DateValue(k.getLeftExpression().toString().substring(6,
													k.getLeftExpression().toString().length() - 2));
											infomap.get("lineitem").setmin(min);
											mini = remi;
										} else if (k instanceof GreaterThan) {
											min = new DateValue(LocalDate
													.parse(k.getLeftExpression().toString().substring(6,
															k.getLeftExpression().toString().length() - 2))
													.plusDays(1).toString());
											infomap.get("lineitem").setmin(min);
											mini = remi;

										} else if (k instanceof MinorThan) {
											max = new DateValue(LocalDate
													.parse(k.getLeftExpression().toString().substring(6,
															k.getLeftExpression().toString().length() - 2))
													.minusDays(1).toString());
											infomap.get("lineitem").setmax(max);
											maxi = remi;
										} else if (k instanceof MinorThanEquals) {
											max = new DateValue(k.getLeftExpression().toString().substring(6,
													k.getLeftExpression().toString().length() - 2));
											infomap.get("lineitem").setmax(max);
											maxi = remi;
										}

									}

									else if (k.getRightExpression().toString().equals("orders.orderdate")) {
										if (k instanceof GreaterThanEquals) {
											min = new DateValue(k.getLeftExpression().toString().substring(6,
													k.getLeftExpression().toString().length() - 2));
											infomap.get("orders").setmin(min);
											mini = remi;
										} else if (k instanceof GreaterThan) {
											min = new DateValue(LocalDate
													.parse(k.getLeftExpression().toString().substring(6,
															k.getLeftExpression().toString().length() - 2))
													.plusDays(1).toString());
											infomap.get("orders").setmin(min);
											mini = remi;
										} else if (k instanceof MinorThan) {
											max = new DateValue(LocalDate
													.parse(k.getLeftExpression().toString().substring(6,
															k.getLeftExpression().toString().length() - 2))
													.minusDays(1).toString());
											infomap.get("orders").setmax(max);
											maxi = remi;
										} else if (k instanceof MinorThanEquals) {
											max = new DateValue(k.getLeftExpression().toString().substring(6,
													k.getLeftExpression().toString().length() - 2));
											infomap.get("orders").setmax(max);
											maxi = remi;
										}

									}

								}
								remi++;

							}

							if (infomap.containsKey("lineitem")) {
								if (infomap.get("lineitem").max != null && infomap.get("lineitem").min != null) {
									ArrayList<PrimitiveValue> submin = new ArrayList();
									submin.add(infomap.get("lineitem").min);
									ArrayList<PrimitiveValue> submax = new ArrayList();
									submax.add(infomap.get("lineitem").max);
									alldt = infomap.get("lineitem").alldt;
									dtypes = infomap.get("lineitem").dtypes;
									// System.out.println(infomap.get("lineitem").max
									// + " " + infomap.get("lineitem").min
									// + " " +
									// indexmap.get("lineitem").get("index_rd").size());
									for (ArrayList<ArrayList<PrimitiveValue>> inp : indexmap.get("lineitem")
											.get("index_rd").subMap(submin, true, submax, true).values()) {
										for (ArrayList<PrimitiveValue> alpv : inp) {
											bkstore.put(alpv, 0);
										}
									}
									bkmap.put("lineitem", bkstore);
									qwe.remove(mini);
									qwe.remove(maxi);
								}
							}
							if (infomap.containsKey("orders")) {
								if (infomap.get("orders").max != null && infomap.get("orders").min != null) {
									ArrayList<PrimitiveValue> submin = new ArrayList();
									submin.add(infomap.get("orders").min);
									ArrayList<PrimitiveValue> submax = new ArrayList();
									submax.add(infomap.get("orders").max);
									alldt = infomap.get("orders").alldt;
									dtypes = infomap.get("orders").dtypes;
									for (ArrayList<ArrayList<PrimitiveValue>> inp : indexmap.get("orders")
											.get("index_od").subMap(submin, true, submax, true).values()) {
										for (ArrayList<PrimitiveValue> alpv : inp) {
											bkstore.put(alpv, 0);
										}
									}
									bkmap.put("orders", bkstore);
									qwe.remove(mini);
									qwe.remove(maxi);

								}
							}

							// System.out.println(qwe + " qwe " + bkstore.size()
							// + "bk size " + bkmap.size());
							for (Expression s : qwe) {
								evlist = new ArrayList();
								ArrayList<PrimitiveValue> bin = new ArrayList();
								BinaryExpression k = (BinaryExpression) s;

								pkkeys.clear();
								whrflag = 0;
								if (k.getLeftExpression() instanceof Column && k.getRightExpression() instanceof Column
										&& !(((Column) k.getLeftExpression()).getTable().getName()
												.equals(((Column) k.getRightExpression()).getTable().getName()))) {
									jlist.add(k);
									continue;
								}
								if (k.getLeftExpression() instanceof Column
										&& k.getRightExpression() instanceof Column) {

									tname = ((Column) k.getLeftExpression()).getTable().getName();
									if (whmap.containsKey(tname)) {
										evlist = whmap.get(tname);
										evlist.add(k);

									} else {
										evlist.add(k);
										whmap.put(tname, evlist);

									}

									// evlist.add(k);
									continue;
								}

								if (k.getLeftExpression() instanceof Column) {
									// System.out.println(((Column)
									// k.getLeftExpression()).getTable().getName());
									tname = ((Column) k.getLeftExpression()).getTable().getName();
								} else {
									tname = ((Column) k.getRightExpression()).getTable().getName();
								}
								prim = infomap.get(tname).prim;
								alldt = infomap.get(tname).alldt;
								dtypes = infomap.get(tname).dtypes;
								data = datamap.get(tname);

								// System.out.println(s);
								if (s.toString().contains(tname + "." + prim.get(0))) {

									whereval++;
									whrflag = 1;
									empflag = 1;
									empmap.put(tname, empflag);

									pkflag = 1;
									if (s instanceof GreaterThanEquals) {
										if (!(((GreaterThanEquals) s).getLeftExpression() instanceof Column)) {
											value = (PrimitiveValue) ((GreaterThanEquals) s).getLeftExpression();
											col = ((GreaterThanEquals) s).getRightExpression().toString();
											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MAX_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.headMap(bin, true).keySet()) {
												pkkeys.put(inp, 0);

											}
											// System.out.println(pkkeys);

										} else {
											col = ((GreaterThanEquals) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((GreaterThanEquals) s).getRightExpression();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MIN_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.tailMap(bin, true).keySet()) {
												pkkeys.put(inp, 0);

											}

										}

									} else if (s instanceof MinorThanEquals) {
										if (!(((MinorThanEquals) s).getLeftExpression() instanceof Column)) {
											value = (PrimitiveValue) ((MinorThanEquals) s).getLeftExpression();
											col = ((MinorThanEquals) s).getRightExpression().toString();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MIN_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.tailMap(bin, true).keySet()) {
												pkkeys.put(inp, 0);

											}

										} else {
											col = ((MinorThanEquals) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((MinorThanEquals) s).getRightExpression();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MAX_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.headMap(bin, true).keySet()) {
												pkkeys.put(inp, 0);

											}

										}
									} else if (s instanceof NotEqualsTo) {
										if (!(((NotEqualsTo) s).getLeftExpression()).toString().contains(prim.get(0))) {
											value = (PrimitiveValue) ((NotEqualsTo) s).getLeftExpression();
											col = ((NotEqualsTo) s).getRightExpression().toString();
										} else {
											col = ((NotEqualsTo) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((NotEqualsTo) s).getRightExpression();
										}

										Iterator<ArrayList<PrimitiveValue>> dt = data.keySet().iterator();
										while (dt.hasNext()) {
											ArrayList<PrimitiveValue> keyp = dt.next();
											if (compstr(keyp.get(0), value) != 0) {
												pkkeys.put(keyp, 0);
												// pkkeys.add((String[])keyp.toArray());

												// pkstore.add((String[])
												// data.get(keyp));
											}
										}

									} else if (s instanceof MinorThan) {
										if (!(((MinorThan) s).getLeftExpression()).toString().contains(prim.get(0))) {
											value = (PrimitiveValue) ((MinorThan) s).getLeftExpression();
											col = ((MinorThan) s).getRightExpression().toString();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MAX_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.tailMap(bin, false).keySet()) {
												pkkeys.put(inp, 0);

											}
											// System.out.println(pkkeys);
										} else {
											col = ((MinorThan) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((MinorThan) s).getRightExpression();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MIN_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.headMap(bin, false).keySet()) {
												pkkeys.put(inp, 0);

											}
										}

									} else if (s instanceof GreaterThan) {

										if (!(((GreaterThan) s).getLeftExpression()).toString().contains(prim.get(0))) {
											value = (PrimitiveValue) ((GreaterThan) s).getLeftExpression();
											col = ((GreaterThan) s).getRightExpression().toString();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MIN_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.headMap(bin, false).keySet()) {
												pkkeys.put(inp, 0);

											}
										} else {
											col = ((GreaterThan) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((GreaterThan) s).getRightExpression();

											bin.add(value);
											if (prim.size() > 1)

												bin.add(new LongValue(Long.MAX_VALUE));

											for (ArrayList<PrimitiveValue> inp : data.tailMap(bin, false).keySet()) {
												pkkeys.put(inp, 0);

											}
										}
									}

									else if (s instanceof EqualsTo) {
										if (!(((EqualsTo) s).getLeftExpression()).toString().contains(prim.get(0))) {
											value = (PrimitiveValue) ((EqualsTo) s).getLeftExpression();
											col = ((EqualsTo) s).getRightExpression().toString();
										} else {
											col = ((EqualsTo) s).getLeftExpression().toString();
											value = (PrimitiveValue) ((EqualsTo) s).getRightExpression();
										}
										ArrayList<PrimitiveValue> maxu = new ArrayList();
										bin.add(value);
										if (prim.size() > 1)

											bin.add(new LongValue(Long.MIN_VALUE));
										maxu.add(new LongValue(value.toLong() + 1));
										if (prim.size() > 1)

											maxu.add(new LongValue(Long.MIN_VALUE));
										for (ArrayList<PrimitiveValue> inp : data.subMap(bin, maxu).keySet()) {
											pkkeys.put(inp, 0);

										}

									}

								} else {
									indexes = infomap.get(tname).indexes;
									Iterator<String> ind = indexes.keySet().iterator();
									while (ind.hasNext()) {
										String obj = ind.next();
										IndexData id = indexes.get(obj);
										// System.out.println(s+" dfgdfg
										// "+id.col.get(0));
										if (s.toString().contains(tname + "." + id.col.get(0))) {

											// System.out.println("assdfsd: " +
											// s + " " + prim + " " + indexes);
											whereval++;
											empflag = 1;
											empmap.put(tname, empflag);
											whrflag = 1;
											index_data = indexmap.get(tname);

											Iterator<ArrayList<PrimitiveValue>> idd = (index_data.get(obj)).keySet()
													.iterator();
											if (s instanceof GreaterThanEquals) {
												if (!(((GreaterThanEquals) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													try {
														value = (PrimitiveValue) ((GreaterThanEquals) s)
																.getLeftExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s).getLeftExpression()
																.toString().substring(6, ((BinaryExpression) s)
																		.getLeftExpression().toString().length() - 2));
													}
													col = ((GreaterThanEquals) s).getRightExpression().toString();

													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.headMap(bin, true).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}

												} else {
													col = ((GreaterThanEquals) s).getLeftExpression().toString();
													try {
														value = (PrimitiveValue) ((GreaterThanEquals) s)
																.getRightExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s)
																.getRightExpression().toString()
																.substring(6, ((BinaryExpression) s)
																		.getRightExpression().toString().length() - 2));
													}
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.tailMap(bin, true).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												}

											} else if (s instanceof MinorThanEquals) {

												if (!(((MinorThanEquals) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													try {
														value = (PrimitiveValue) ((MinorThanEquals) s)
																.getLeftExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s).getLeftExpression()
																.toString().substring(6, ((BinaryExpression) s)
																		.getLeftExpression().toString().length() - 2));
													}
													col = ((MinorThanEquals) s).getRightExpression().toString();

													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.tailMap(bin, true).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												} else {

													col = ((MinorThanEquals) s).getLeftExpression().toString();
													try {
														value = (PrimitiveValue) ((MinorThanEquals) s)
																.getRightExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s)
																.getRightExpression().toString()
																.substring(6, ((BinaryExpression) s)
																		.getRightExpression().toString().length() - 2));
													}

													// value =
													// String.valueOf((Integer.parseInt(value)
													// + 1));
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.headMap(bin, true).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												}
											} else if (s instanceof NotEqualsTo) {
												if (!(((NotEqualsTo) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													value = (PrimitiveValue) ((NotEqualsTo) s).getLeftExpression();
													col = ((NotEqualsTo) s).getRightExpression().toString();
												} else {
													col = ((NotEqualsTo) s).getLeftExpression().toString();
													value = (PrimitiveValue) ((NotEqualsTo) s).getRightExpression();
												}
												while (idd.hasNext()) {
													ArrayList<PrimitiveValue> keyp = idd.next();
													if (compstr(keyp.get(0), value, id.seq.get(0)) != 0) {
														ArrayList<ArrayList<PrimitiveValue>> indexd = (index_data
																.get(obj)).get(keyp);
														Iterator<ArrayList<PrimitiveValue>> itr = indexd.iterator();
														while (itr.hasNext()) {
															ArrayList<PrimitiveValue> kp = itr.next();
															pkkeys.put(kp, 0);
															// pkkeys.add((String[])kp.toArray());
															// pkstore.add((String[])
															// data.get(kp));
														}
													}

												}

											} else if (s instanceof MinorThan) {
												if (!(((MinorThan) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													try {

														value = (PrimitiveValue) ((MinorThan) s).getLeftExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s).getLeftExpression()
																.toString().substring(6, ((BinaryExpression) s)
																		.getLeftExpression().toString().length() - 2));
													}

													col = ((MinorThan) s).getRightExpression().toString();
													// value =
													// String.valueOf((Integer.parseInt(value)
													// + 1));
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.tailMap(bin, false).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												} else {

													col = ((MinorThan) s).getLeftExpression().toString();
													try {
														value = (PrimitiveValue) ((MinorThan) s).getRightExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s)
																.getRightExpression().toString()
																.substring(6, ((BinaryExpression) s)
																		.getRightExpression().toString().length() - 2));
													}
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.headMap(bin, false).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												}
											} else if (s instanceof GreaterThan) {
												if (!(((GreaterThan) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													try {
														value = (PrimitiveValue) ((GreaterThan) s).getLeftExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s).getLeftExpression()
																.toString().substring(6, ((BinaryExpression) s)
																		.getLeftExpression().toString().length() - 2));
													}
													col = ((GreaterThan) s).getRightExpression().toString();
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.headMap(bin, false).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												} else {
													col = ((GreaterThan) s).getLeftExpression().toString();
													try {
														value = (PrimitiveValue) ((GreaterThan) s).getRightExpression();
													} catch (Exception e) {
														value = new DateValue(((BinaryExpression) s)
																.getRightExpression().toString()
																.substring(6, ((BinaryExpression) s)
																		.getRightExpression().toString().length() - 2));
													}
													// value =
													// String.valueOf((Integer.parseInt(value)
													// + 1));
													bin.add(value);

													for (ArrayList<ArrayList<PrimitiveValue>> inp : index_data.get(obj)
															.tailMap(bin, false).values()) {
														for (ArrayList<PrimitiveValue> alpv : inp) {
															pkkeys.put(alpv, 0);
														}
													}
												}
											}

											else if (s instanceof EqualsTo) {
												if (!(((EqualsTo) s).getLeftExpression()).toString()
														.contains(id.col.get(0))) {
													value = (PrimitiveValue) ((EqualsTo) s).getLeftExpression();
													col = ((EqualsTo) s).getRightExpression().toString();
												} else {
													col = ((EqualsTo) s).getLeftExpression().toString();
													value = (PrimitiveValue) ((EqualsTo) s).getRightExpression();
												}

												while (idd.hasNext()) {
													ArrayList<PrimitiveValue> keyp = idd.next();
													if (compstr(keyp.get(0), value, id.seq.get(0)) == 0) {
														ArrayList<ArrayList<PrimitiveValue>> indexd = (index_data
																.get(obj)).get(keyp);
														Iterator<ArrayList<PrimitiveValue>> itr = indexd.iterator();
														while (itr.hasNext()) {
															ArrayList<PrimitiveValue> kp = itr.next();
															pkkeys.put(kp, 0);
															// pkkeys.add((String[])kp.toArray());
															// pkstore.add((String[])
															// data.get(kp));
														}
														break;
													}
												}
											}
											break;
										}

									}
								}
								// System.out.println(whrflag + "asd " +
								// bkstore.size() + " " + pkkeys.size());
								if (whrflag == 0) {
									// tname = ((Column)
									// k.getLeftExpression()).getTable().getName();
									if (whmap.containsKey(tname)) {
										evlist = whmap.get(tname);
										evlist.add(k);

									} else {
										evlist.add(k);
										whmap.put(tname, evlist);

									}

									// evlist.add(k);
								}

								if (!bkmap.containsKey(tname) && whrflag == 1) {
									if (!pkkeys.isEmpty()) {
										bkstore = (HashMap<ArrayList<PrimitiveValue>, Integer>) pkkeys.clone();
										bkmap.put(tname, bkstore);
										// System.out.println(whrflag + " bk " +
										// bkstore.size() +" "+ pkkeys.size());
									} else {
										resflag = 1;
										break;
									}

								} else if (whrflag == 1) {
									HashMap<ArrayList<PrimitiveValue>, Integer> temps = new HashMap();
									bkstore = bkmap.get(tname);
									if (!pkkeys.isEmpty()) {

										// System.out.println("sdf"+bkstore.size());
										if (bkstore.size() > pkkeys.size()) {
											for (ArrayList<PrimitiveValue> w : pkkeys.keySet()) {
												if (bkstore.containsKey(w)) {
													temps.put(w, 0);

												}

											}

										} else {
											for (ArrayList<PrimitiveValue> w : bkstore.keySet()) {
												if (pkkeys.containsKey(w)) {
													temps.put(w, 0);

												}

											}

										}
										if (temps.size() == 0) {
											bkstore.clear();
											resflag = 1;
											break;
										}

										bkstore.clear();
										bkstore = (HashMap<ArrayList<PrimitiveValue>, Integer>) temps.clone();
										bkmap.put(tname, bkstore);

									} else {
										resflag = 1;
										bkstore.clear();
										break;
									}

								}

							}

						}

						if (qflag == 0) {

							Iterator<String> itm = bkmap.keySet().iterator();
							while (itm.hasNext()) {
								pkstore = new ArrayList();
								String key = itm.next();
								bkstore = bkmap.get(key);
								alldt = infomap.get(key).alldt;
								dtypes = infomap.get(key).dtypes;
								data = datamap.get(key);

								if (bkstore.isEmpty() && !empmap.containsKey(key)) {

									pkstore = new ArrayList(data.values());
									pkmap.put(key, pkstore);

								} else if (bkstore.isEmpty()) {
									resflag = 1;
									break;
								} else {
									Iterator bk = bkstore.keySet().iterator();
									while (bk.hasNext()) {
										pkstore.add(data.get(bk.next()));

									}
									pkmap.put(key, pkstore);

								}

							}

						}
						// System.out.println(pkmap.size() + " p " +
						// pkstore.size() + " j " + jlist + " e " + whmap);

						Iterator<String> pku = whmap.keySet().iterator();
						while (pku.hasNext()) {
							String key = pku.next();
							alldt = infomap.get(key).alldt;
							dtypes = infomap.get(key).dtypes;
							tname = key;

							if (!pkmap.containsKey(key)) {
								if (order.toString().contains("lineitem.returnflag")) {
									resflag=1;
									/*
									 * data = datamap.get(key); pkstore = new
									 * ArrayList();
									 * Collection<ArrayList<ArrayList<
									 * PrimitiveValue>>> df =
									 * indexmap.get("lineitem")
									 * .get("index_lin").values(); for
									 * (ArrayList<ArrayList<PrimitiveValue>> mf
									 * : df) { for (ArrayList<PrimitiveValue> gh
									 * : mf) { pkstore.add(data.get(gh));
									 * 
									 * }
									 * 
									 * } pkmap.put(key, pkstore);
									 */

									int counter = 0;
									int mycount = 0;
									int grpflag = 0;
									int numAggs = selectitem.size();
									
									int minflag = 0, i = mincount;
									ArrayList<PrimitiveValue> prev = new ArrayList();

									  //Set<Entry<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>>> df = indexmap.get("lineitem")
											//.get("index_lin").entrySet();
									  
									for (Entry<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>> mft : indexmap.get("lineitem")
											.get("index_lin").entrySet()) {
										ArrayList<ArrayList<PrimitiveValue>> mf = mft.getValue();
										ArrayList<PrimitiveValue> curr = new ArrayList();
										Double totalArr[] = new Double[numAggs];
										Arrays.fill(totalArr, 0.0);
										Iterator<Column> grp = group.iterator();
										int j=0;
										while (grp.hasNext()) {
											Column colgrp = (Column) grp.next();
											
											curr.add(mft.getKey().get(j++));
											
										}
										for (ArrayList<PrimitiveValue> gh : mf) {
											plinevals = data.get(gh);
											PrimitiveValue result = null;
											int nof = 0;
											Iterator<BinaryExpression> wh = whmap.get(key).iterator();
											while (wh.hasNext()) {
												nof = 0;
												result = eval.eval(wh.next());
												if (result.toBool()) {
													nof = 1;
												} else {

													break;

												}

											}
											if (nof == 1) {

												
												grpflag = 1;
												// int ins=0;
												// String[] insert=new
												// String[selectitem.size()];
												/*if (!curr.equals(prev)) {
													long ctr;

													if (lt != null) {
														ctr = lt.getRowCount();
													} else
														ctr = Long.MAX_VALUE;
													if (mycount == ctr)
														break;
													mycount++;

													Iterator<PrimitiveValue> previous = prev.iterator();
													ctr++;
													while (previous.hasNext()) {
														System.out.print(previous.next() + "|");
														// insert[ins++] =
														// previous.next();
													}

													for (int k = 0; k < numAggs; k++) {
														if (totalArr[k] != -1.0) {
															System.out.print(totalArr[k] + "|");
															// insert[ins++] =
															// new
															// DoubleValue(totalArr[k]);
														}

													}
													System.out.println("\b");
													Arrays.fill(totalArr, 0.0);
													minflag = 0;
													counter = 0;
													prev = (ArrayList<PrimitiveValue>) curr.clone();

												}*/

												Iterator<SelectItem> si = selectitem.iterator();
												Iterator<Integer> comparei = compare1.iterator();
												int index = 0;
												counter++;

												while (si.hasNext()) {
													SelectItem selectFrom = si.next();
													int nom = comparei.next();
													if (nom == 0) {
														totalArr[index] = -1.0;
														index++;
														continue;

													}
													Expression e = ((SelectExpressionItem) selectFrom).getExpression();

													Function f = (Function) e;

													if ((nom == 1)) {
														PrimitiveValue res = eval
																.eval(f.getParameters().getExpressions().get(0));
														totalArr[index] = totalArr[index] + res.toDouble();
														index++;
													} else if (nom == 4) {
														totalArr[index]++;
														index++;
													} else if (nom == 3) {
														PrimitiveValue res = eval
																.eval(f.getParameters().getExpressions().get(0));
														if (res.toDouble() > totalArr[index])
															totalArr[index] = res.toDouble();
														index++;
													} else if (nom == 2) {
														if (minflag < i) {
															// System.out.println(""+minflag+i);
															totalArr[index] = Double.MAX_VALUE;
															minflag++;
														}
														PrimitiveValue res = eval
																.eval(f.getParameters().getExpressions().get(0));
														if (res.toDouble() < totalArr[index])
															totalArr[index] = res.toDouble();
														index++;
													} else if (nom == 5) {
														PrimitiveValue res = eval
																.eval(f.getParameters().getExpressions().get(0));
														totalArr[index] = totalArr[index] + res.toDouble();
														index++;

													}
												}

											}

										}

										Iterator<PrimitiveValue> previous = curr.iterator();
										while (previous.hasNext()) {
											System.out.print(previous.next() + "|");
											// insert[ins++] = previous.next();
										}

										for (int k = 2; k < numAggs; k++) {
												if (k == 7 || k == 8 || k == 6) {
													System.out.print(totalArr[k] / totalArr[9] + "|");
												} else {
													System.out.print(totalArr[k] + "|");
												}
												// insert[ins++] = new
												// DoubleValue(totalArr[k]);
											

										}
										System.out.println("\b");
										Arrays.fill(totalArr, 0.0);
										minflag = 0;
										counter = 0;

									}

								} else {
									data = datamap.get(key);
									pkstore = new ArrayList(data.values());
									pkmap.put(key, pkstore);
								}
							} else {
								pkstore = pkmap.get(key);
							}
							Iterator<PrimitiveValue[]> itm = pkstore.iterator();
							ArrayList<PrimitiveValue[]> tempu = new ArrayList();
							while (itm.hasNext()) {
								plinevals = itm.next();
								PrimitiveValue result = null;
								int nof = 0;
								Iterator<BinaryExpression> wh = whmap.get(key).iterator();
								while (wh.hasNext()) {
									nof = 0;
									result = eval.eval(wh.next());
									if (result.toBool()) {
										nof = 1;
									} else {

										break;

									}

								}
								if (nof == 1) {
									tempu.add(plinevals);

								}

							}
							// System.out.println("Refined: " + tempu.size());
							if (tempu.size() == 0) {
								resflag = 1;
								break;
							}
							pkmap.put(key, tempu);

						}
						if (resflag == 1)
							break;

						if (!pkmap.containsKey(ps.getFromItem().toString())) {

							pkmap.put(ps.getFromItem().toString(),
									new ArrayList(datamap.get(ps.getFromItem().toString()).values()));
						}
						ArrayList<HashMap<String, PrimitiveValue[]>> join = new ArrayList();
						ArrayList<HashMap<String, PrimitiveValue[]>> injoin = new ArrayList();
						if (ps.getJoins() != null) {
							Iterator<Join> jit = ps.getJoins().iterator();
							while (jit.hasNext()) {
								String key = jit.next().toString();
								if (!pkmap.containsKey(key)) {
									pkmap.put(key, new ArrayList(datamap.get(key).values()));
								}
							}
							Collections.reverse(jlist);

							Iterator<BinaryExpression> jadu = jlist.iterator();
							int jflag = 0;
							while (jadu.hasNext()) {
								HashMap<PrimitiveValue, ArrayList<HashMap<String, PrimitiveValue[]>>> joinmap = new HashMap();
								BinaryExpression k = jadu.next();

								if (jflag == 0) {
									tname = ((Column) k.getLeftExpression()).getTable().getName();
									String lname = tname;
									String coln = k.getLeftExpression().toString();
									pkstore = pkmap.get(tname);
									int sq = schema.get(tname).get(coln).seq;
									Iterator<PrimitiveValue[]> pki = pkstore.iterator();
									while (pki.hasNext()) {
										PrimitiveValue[] val = pki.next();
										ArrayList<HashMap<String, PrimitiveValue[]>> jf = new ArrayList();
										if (joinmap.containsKey(val[sq])) {
											HashMap<String, PrimitiveValue[]> sdf = new HashMap();
											jf = joinmap.get(val[sq]);
											sdf.put(lname, val);
											jf.add(sdf);

										} else {
											HashMap<String, PrimitiveValue[]> sdf = new HashMap();
											sdf.put(lname, val);
											jf.add(sdf);
											joinmap.put(val[sq], jf);
										}
									}

									tname = ((Column) k.getRightExpression()).getTable().getName();
									coln = k.getRightExpression().toString();
									pkstore = pkmap.get(tname);
									sq = schema.get(tname).get(coln).seq;
									pki = pkstore.iterator();
									while (pki.hasNext()) {
										PrimitiveValue[] val = pki.next();
										if (joinmap.containsKey(val[sq])) {
											ArrayList<HashMap<String, PrimitiveValue[]>> bk = (ArrayList<HashMap<String, PrimitiveValue[]>>) joinmap
													.get(val[sq]).clone();
											Iterator<HashMap<String, PrimitiveValue[]>> bm = bk.iterator();
											while (bm.hasNext()) {
												HashMap<String, PrimitiveValue[]> put = bm.next();
												HashMap<String, PrimitiveValue[]> putnew = new HashMap<String, PrimitiveValue[]>();
												Iterator<String> hashit = put.keySet().iterator();
												while (hashit.hasNext()) {
													String pva = hashit.next();
													putnew.put(pva, put.get(pva));
												}
												putnew.put(tname, val);
												join.add(putnew);

											}

										}

									}

									jflag = 1;

								} else {
									if (join.size() > 0) {
										HashMap bsd = join.get(0);
										tname = ((Column) k.getLeftExpression()).getTable().getName();
										String rname = ((Column) k.getRightExpression()).getTable().getName();
										String lname = tname;
										String coln = k.getLeftExpression().toString();
										int sq = schema.get(lname).get(coln).seq;

										if (bsd.keySet().contains(lname) && bsd.keySet().contains(rname)) {

											String rcoln = k.getRightExpression().toString();
											int rsq = schema.get(rname).get(rcoln).seq;

											Iterator<HashMap<String, PrimitiveValue[]>> joinit = join.iterator();
											while (joinit.hasNext()) {
												HashMap<String, PrimitiveValue[]> jabs = joinit.next();
												PrimitiveValue lrow = jabs.get(lname)[sq];
												PrimitiveValue rrow = jabs.get(rname)[rsq];
												if (lrow.equals(rrow)) {
													injoin.add(jabs);
												}
											}

										} else {
											if (bsd.keySet().contains(lname)) {
												Iterator<HashMap<String, PrimitiveValue[]>> jm = join.iterator();
												while (jm.hasNext()) {
													HashMap<String, PrimitiveValue[]> hm = jm.next();
													PrimitiveValue[] arr = hm.get(lname);
													ArrayList<HashMap<String, PrimitiveValue[]>> jf = new ArrayList();
													if (joinmap.containsKey(arr[sq])) {
														jf = joinmap.get(arr[sq]);
														jf.add(hm);

													} else {
														jf.add(hm);
														joinmap.put(arr[sq], jf);

													}

												}

											} else {

												pkstore = pkmap.get(lname);

												Iterator<PrimitiveValue[]> pki = pkstore.iterator();
												while (pki.hasNext()) {
													PrimitiveValue[] val = pki.next();
													ArrayList<HashMap<String, PrimitiveValue[]>> jf = new ArrayList();
													if (joinmap.containsKey(val[sq])) {
														HashMap<String, PrimitiveValue[]> sdf = new HashMap();

														jf = joinmap.get(val[sq]);
														sdf.put(lname, val);
														jf.add(sdf);

													} else {
														HashMap<String, PrimitiveValue[]> sdf = new HashMap();
														sdf.put(lname, val);
														jf.add(sdf);
														joinmap.put(val[sq], jf);
													}
												}

											}

											tname = ((Column) k.getRightExpression()).getTable().getName();
											coln = k.getRightExpression().toString();
											sq = schema.get(tname).get(coln).seq;
											if (bsd.keySet().contains(tname)) {
												Iterator<HashMap<String, PrimitiveValue[]>> jm = join.iterator();
												while (jm.hasNext()) {
													HashMap<String, PrimitiveValue[]> hm = jm.next();
													PrimitiveValue[] arr = hm.get(tname);
													ArrayList<HashMap<String, PrimitiveValue[]>> jf = new ArrayList();
													if (joinmap.containsKey(arr[sq])) {
														Iterator<HashMap<String, PrimitiveValue[]>> jk = joinmap
																.get(arr[sq]).iterator();

														while (jk.hasNext()) {
															HashMap<String, PrimitiveValue[]> sdf = jk.next();
															Iterator<String> hmit = hm.keySet().iterator();
															while (hmit.hasNext()) {
																String hname = hmit.next();
																sdf.put(hname, hm.get(hname));
															}
															injoin.add(sdf);
														}

													}
												}

											} else {

												pkstore = pkmap.get(tname);

												Iterator<PrimitiveValue[]> pki = pkstore.iterator();
												while (pki.hasNext()) {
													PrimitiveValue[] val = pki.next();
													if (joinmap.containsKey(val[sq])) {
														ArrayList<HashMap<String, PrimitiveValue[]>> bk = joinmap
																.get(val[sq]);
														Iterator<HashMap<String, PrimitiveValue[]>> bm = bk.iterator();
														while (bm.hasNext()) {
															HashMap<String, PrimitiveValue[]> put = bm.next();
															HashMap<String, PrimitiveValue[]> putnew = new HashMap<String, PrimitiveValue[]>();
															Iterator<String> hashit = put.keySet().iterator();
															while (hashit.hasNext()) {
																String pva = hashit.next();
																putnew.put(pva, put.get(pva));
															}
															putnew.put(tname, val);
															injoin.add(putnew);

														}

													}

												}

											}

										}
									}
									join = new ArrayList();
									join = (ArrayList<HashMap<String, PrimitiveValue[]>>) injoin.clone();
									injoin = new ArrayList();
								}

							}
							pkstore = new ArrayList<PrimitiveValue[]>();
							ArrayList<Integer> grpsq = new ArrayList<>();
							ArrayList<Integer> asq = new ArrayList<>();
							HashMap<ArrayList<PrimitiveValue>, ArrayList<Double>> gdata = new HashMap();
							int m = 0;
							Iterator<HashMap<String, PrimitiveValue[]>> getrow = join.iterator();
							int mflag = 0;

							while (getrow.hasNext()) {
								PrimitiveValue[] gm = new PrimitiveValue[selectitem.size()];
								rowmap = getrow.next();
								Iterator<SelectItem> lis = selectitem.iterator();
								m = 0;
								while (lis.hasNext()) {

									SelectItem gplist = lis.next();
									Expression e = ((SelectExpressionItem) gplist).getExpression();

									if (e instanceof Function) {
										if (mflag == 0) {
											asq.add(m);
										}
										Function res = (Function) e;
										if (res.getName().equals("count")) {
											gm[m++] = new LongValue(1);

										} else {

											Expression fi = res.getParameters().getExpressions().get(0);
											// System.out.println(fi);

											checkf = 1;
											gm[m++] = eval.eval(fi);
											checkf = 0;

										}

									} else {
										if (mflag == 0) {
											grpsq.add(m);
										}
										String tn = ((Column) (e)).getTable().getName();
										PrimitiveValue[] km = rowmap.get(tn);
										int sq = schema.get(tn).get(gplist.toString()).seq;
										gm[m++] = km[sq];

									}

								}
								mflag = 1;
								ArrayList<PrimitiveValue> keys = new ArrayList();
								Iterator<Integer> gsq = grpsq.iterator();
								while (gsq.hasNext()) {
									keys.add(gm[gsq.next()]);
								}
								if (gdata.containsKey(keys)) {

									ArrayList<Double> sc = gdata.get(keys);
									Iterator<Integer> asqi = asq.iterator();
									int ind = 0;
									while (asqi.hasNext()) {
										int sq = asqi.next();
										int tp = compare1.get(sq);
										Double dval = sc.get(ind);
										if (tp == 1) {
											dval += gm[sq].toDouble();
										} else if (tp == 2) {
											if (gm[sq].toDouble() < dval) {
												dval = gm[sq].toDouble();
											}

										} else if (tp == 3) {
											if (gm[sq].toDouble() > dval) {
												dval = gm[sq].toDouble();
											}

										} else if (tp == 4) {
											dval++;

										} else if (tp == 5) {
											// avg
										}

										sc.set(ind, dval);

										ind++;
									}
									gdata.put(keys, sc);

								} else {
									ArrayList<Double> sc = new ArrayList();
									Iterator<Integer> asqi = asq.iterator();
									while (asqi.hasNext()) {
										sc.add(gm[asqi.next()].toDouble());
									}
									gdata.put(keys, sc);
								}

							}

							for (ArrayList<PrimitiveValue> fugi : gdata.keySet()) {
								PrimitiveValue[] arrr = new PrimitiveValue[selectitem.size()];
								Iterator<Integer> gsq = grpsq.iterator();
								int find = 0;
								while (gsq.hasNext()) {
									arrr[gsq.next()] = fugi.get(find);
									find++;
								}
								gsq = asq.iterator();
								find = 0;
								ArrayList<Double> qwa = gdata.get(fugi);
								while (gsq.hasNext()) {

									arrr[gsq.next()] = new DoubleValue(qwa.get(find));
									find++;
								}

								pkstore.add(arrr);

							}
							if (order != null) {
								List<Integer> osq = new ArrayList();
								for (OrderByElement s : order) {
									int os = 0;
									Iterator<SelectItem> lis = selectitem.iterator();

									while (lis.hasNext()) {
										SelectItem we = lis.next();
										String of = ((SelectExpressionItem) we).getAlias();

										if ((of != null && of.equals(s.getExpression().toString()))
												|| we.toString().equals(s.getExpression().toString())) {
											osq.add(os);
											break;
										}
										os++;

									}
								}

								Collections.sort(pkstore, new Comparator<PrimitiveValue[]>() {

									@Override
									public int compare(PrimitiveValue[] arg0, PrimitiveValue[] arg1) {
										int a = 0;
										for (OrderByElement s : order) {

											int seq = osq.get(a);
											a++;
											int cmp = 0;

											try {

												if (arg0[seq] instanceof LongValue) {

													cmp = Long.compare(arg0[seq].toLong(), arg1[seq].toLong());

												} else if (arg0[seq] instanceof StringValue) {
													cmp = arg0[seq].toString().compareTo(arg1[seq].toString());
												} else if (arg0[seq] instanceof DoubleValue) {
													cmp = Double.compare(arg0[seq].toDouble(), arg1[seq].toDouble());
												} else if (arg0[seq] instanceof DateValue) {

													cmp = ((DateValue) arg0[seq]).getValue()
															.compareTo(((DateValue) arg1[seq]).getValue());

												}

												if (cmp != 0) {
													if (s.isAsc())
														return cmp;
													else
														return -cmp;
												}
											} catch (Exception e) {
											}

										}
										return 0;
									}
								});

							}
							if (ps.getLimit() != null) {
								backpk = new ArrayList(pkstore.subList(0, (int) ps.getLimit().getRowCount()));
							} else {
								backpk = (ArrayList<PrimitiveValue[]>) pkstore.clone();
							}
						}
						// System.out.println(backpk.size());

						if (false) {
							pkstore = new ArrayList<PrimitiveValue[]>();
							ArrayList<Integer> grpsq = new ArrayList<>();
							ArrayList<Integer> asq = new ArrayList<>();
							HashMap<ArrayList<PrimitiveValue>, ArrayList<Double>> gdata = new HashMap();
							int m = 0;
							Iterator<PrimitiveValue[]> getrow = pkmap.get("lineitem").iterator();
							int mflag = 0;

							while (getrow.hasNext()) {
								PrimitiveValue[] gm = new PrimitiveValue[selectitem.size()];
								plinevals = getrow.next();
								Iterator<SelectItem> lis = selectitem.iterator();
								m = 0;
								while (lis.hasNext()) {

									SelectItem gplist = lis.next();
									Expression e = ((SelectExpressionItem) gplist).getExpression();

									if (e instanceof Function) {
										if (mflag == 0) {
											asq.add(m);
										}
										Function res = (Function) e;
										if (res.getName().equals("count")) {
											gm[m++] = new LongValue(1);

										} else {

											Expression fi = res.getParameters().getExpressions().get(0);

											gm[m++] = eval.eval(fi);

										}

									} else {
										if (mflag == 0) {
											grpsq.add(m);
										}
										String tn = ((Column) (e)).getTable().getName();
										int sq = schema.get(tn).get(gplist.toString()).seq;
										gm[m++] = plinevals[sq];

									}

								}
								mflag = 1;
								ArrayList<PrimitiveValue> keys = new ArrayList();
								Iterator<Integer> gsq = grpsq.iterator();
								while (gsq.hasNext()) {
									keys.add(gm[gsq.next()]);
								}
								if (gdata.containsKey(keys)) {

									ArrayList<Double> sc = gdata.get(keys);
									Iterator<Integer> asqi = asq.iterator();
									int ind = 0;
									while (asqi.hasNext()) {
										int sq = asqi.next();
										int tp = compare1.get(sq);
										Double dval = sc.get(ind);
										if (tp == 1) {
											dval += gm[sq].toDouble();
										} else if (tp == 2) {
											if (gm[sq].toDouble() < dval) {
												dval = gm[sq].toDouble();
											}

										} else if (tp == 3) {
											if (gm[sq].toDouble() > dval) {
												dval = gm[sq].toDouble();
											}

										} else if (tp == 4) {
											dval++;

										} else if (tp == 5) {
											dval = (dval * sc.get(7) + gm[sq].toDouble()) / (sc.get(7) + 1);
										}

										sc.set(ind, dval);

										ind++;
									}
									gdata.put(keys, sc);

								} else {
									ArrayList<Double> sc = new ArrayList();
									Iterator<Integer> asqi = asq.iterator();
									while (asqi.hasNext()) {
										sc.add(gm[asqi.next()].toDouble());
									}
									gdata.put(keys, sc);
								}

							}

							for (ArrayList<PrimitiveValue> fugi : gdata.keySet()) {
								PrimitiveValue[] arrr = new PrimitiveValue[selectitem.size()];
								Iterator<Integer> gsq = grpsq.iterator();
								int find = 0;
								while (gsq.hasNext()) {
									arrr[gsq.next()] = fugi.get(find);
									find++;
								}
								gsq = asq.iterator();
								find = 0;
								ArrayList<Double> qwa = gdata.get(fugi);
								while (gsq.hasNext()) {

									arrr[gsq.next()] = new DoubleValue(qwa.get(find));
									find++;
								}

								pkstore.add(arrr);

							}
							if (order != null) {
								List<Integer> osq = new ArrayList();
								for (OrderByElement s : order) {
									int os = 0;
									Iterator<SelectItem> lis = selectitem.iterator();

									while (lis.hasNext()) {
										SelectItem we = lis.next();
										String of = ((SelectExpressionItem) we).getAlias();

										if ((of != null && of.equals(s.getExpression().toString()))
												|| we.toString().equals(s.getExpression().toString())) {
											osq.add(os);
											break;
										}
										os++;

									}
								}

								Collections.sort(pkstore, new Comparator<PrimitiveValue[]>() {

									@Override
									public int compare(PrimitiveValue[] arg0, PrimitiveValue[] arg1) {
										int a = 0;
										for (OrderByElement s : order) {

											int seq = osq.get(a);
											a++;
											int cmp = 0;

											try {

												if (arg0[seq] instanceof LongValue) {

													cmp = Long.compare(arg0[seq].toLong(), arg1[seq].toLong());

												} else if (arg0[seq] instanceof StringValue) {
													cmp = arg0[seq].toString().compareTo(arg1[seq].toString());
												} else if (arg0[seq] instanceof DoubleValue) {
													cmp = Double.compare(arg0[seq].toDouble(), arg1[seq].toDouble());
												} else if (arg0[seq] instanceof DateValue) {

													cmp = ((DateValue) arg0[seq]).getValue()
															.compareTo(((DateValue) arg1[seq]).getValue());

												}

												if (cmp != 0) {
													if (s.isAsc())
														return cmp;
													else
														return -cmp;
												}
											} catch (Exception e) {
											}

										}
										return 0;
									}
								});

							}
							if (ps.getLimit() != null) {
								backpk = new ArrayList(pkstore.subList(0, (int) ps.getLimit().getRowCount()));
							} else {
								backpk = (ArrayList<PrimitiveValue[]>) pkstore.clone();
							}
						}

						if (false) {
							// System.out.println(tname+"."+prim.get(0)+"
							// "+order.get(0));
							pkstore = pkmap.get(tname);
							Collections.sort(pkstore, new Comparator<PrimitiveValue[]>() {

								@Override
								public int compare(PrimitiveValue[] arg0, PrimitiveValue[] arg1) {
									for (OrderByElement s : order) {

										int seq = (schema.get(tname)).get(s.getExpression().toString()).seq;
										int cmp = 0;
										String datatype = alldt.get(seq);
										try {

											if (datatype.equals("int")) {

												cmp = Long.compare(arg0[seq].toLong(), arg1[seq].toLong());

											} else if (datatype.equals("string") || datatype.equals("varchar")
													|| datatype.equals("char")) {
												cmp = arg0[seq].toString().compareTo(arg1[seq].toString());
											} else if (datatype.equals("decimal")) {
												cmp = Double.compare(arg0[seq].toDouble(), arg1[seq].toDouble());
											} else if (datatype.equals("date")) {

												cmp = ((DateValue) arg0[seq]).getValue()
														.compareTo(((DateValue) arg1[seq]).getValue());

											}

											if (cmp != 0) {
												if (s.isAsc())
													return cmp;
												else
													return -cmp;
											}
										} catch (Exception e) {
										}

									}
									return 0;
								}
							});
						}

						else if (false) {
							pkstore = pkmap.get(tname);
							Collections.sort(pkstore, new Comparator<PrimitiveValue[]>() {

								@Override
								public int compare(PrimitiveValue[] arg0, PrimitiveValue[] arg1) {
									for (Column s : group) {
										int seq = (schema.get(tname)).get(s.toString()).seq;
										int cmp = 0;
										String datatype = alldt.get(seq);

										try {
											if (datatype.equals("int")) {

												cmp = Long.compare(arg0[seq].toLong(), arg1[seq].toLong());

											} else if (datatype.equals("string") || datatype.equals("varchar")
													|| datatype.equals("char")) {
												cmp = arg0[seq].toString().compareTo(arg1[seq].toString());
											} else if (datatype.equals("decimal")) {
												cmp = Double.compare(arg0[seq].toDouble(), arg1[seq].toDouble());
											} else if (datatype.equals("date")) {

												cmp = ((DateValue) arg0[seq]).getValue()
														.compareTo(((DateValue) arg1[seq]).getValue());

											}

											if (cmp != 0) {
												return cmp;

											}

										} catch (Exception e) {
										}
									}
									return 0;
								}
							});

						}

						if (ps.getJoins() == null) {
							pkstore = pkmap.get(tname);

							Iterator<PrimitiveValue[]> pkit = pkstore.iterator();
							PrimitiveValue result = null;
							int counter = 0;
							int mycount = 0;
							int grpflag = 0;
							int numAggs = selectitem.size();
							Double totalArr[] = new Double[numAggs];
							Arrays.fill(totalArr, 0.0);
							int minflag = 0, i = mincount;
							LinkedHashMap<String, ColData> colmap = schema.get(tname);

							ArrayList prev = new ArrayList();
							while (pkit.hasNext()) {
								int ins = 0;
								PrimitiveValue[] insert = new PrimitiveValue[selectitem.size()];

								plinevals = pkit.next();

								if (group == null && flag == 1)
									rcflag = 1;
								// System.out.println("nof null"+nof+" r
								// "+result);

								if (flag == 0) {

									// insert=new String[selectitem.size()];
									// no aggregation
									if (lt != null) {

										if (counter < lt.getRowCount()) {
											counter++;
											Iterator<SelectItem> selit = selectitem.iterator();
											while (selit.hasNext()) {

												Object selectFrom = selit.next();

												insert[ins++] = eval
														.eval(((SelectExpressionItem) selectFrom).getExpression());

											}

										} else
											break;
									} else {

										Iterator<SelectItem> selit = selectitem.iterator();
										// System.out.println("print 1 limit
										// null");
										while (selit.hasNext()) {
											Object selectFrom = selit.next();
											insert[ins++] = eval
													.eval(((SelectExpressionItem) selectFrom).getExpression());

										}
									}
								} else {
									if (group == null) {
										Iterator<SelectItem> si = selectitem.iterator();
										Iterator<Integer> comparei = compare1.iterator();
										int index = 0;
										counter++;
										while (si.hasNext()) {
											SelectExpressionItem selectFrom = (SelectExpressionItem) si.next();
											Expression e = selectFrom.getExpression();
											Function f = (Function) e;
											int nom = comparei.next();
											if ((nom == 1)) {
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												totalArr[index] = totalArr[index] + res.toDouble();
												index++;
											} else if (nom == 4) {
												totalArr[index]++;
												index++;
											} else if (nom == 3) {

												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												if (res.toDouble() > totalArr[index])
													totalArr[index] = res.toDouble();
												index++;
											} else if (nom == 2) {
												if (minflag < i) {
													// System.out.println(""+minflag+i);
													totalArr[index] = Double.MAX_VALUE;
													minflag++;
												}
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												if (res.toDouble() < totalArr[index])
													totalArr[index] = res.toDouble();
												index++;
											} else if (nom == 5) {
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												totalArr[index] = totalArr[index] * (counter - 1) + res.toDouble();
												totalArr[index] = totalArr[index] / counter;
												index++;

											}
										}
									} else {
										ArrayList<PrimitiveValue> curr = new ArrayList();
										Iterator<Column> grp = group.iterator();
										while (grp.hasNext()) {
											Column colgrp = (Column) grp.next();
											int seq = (schema.get(tname)).get(colgrp.toString()).seq;
											curr.add(plinevals[seq]);
											if (grpflag == 0) {
												prev.add(plinevals[seq]);
											}

										}
										grpflag = 1;
										// int ins=0;
										// String[] insert=new
										// String[selectitem.size()];
										if (!curr.equals(prev)) {
											long ctr;

											if (lt != null) {
												ctr = lt.getRowCount();
											} else
												ctr = Long.MAX_VALUE;
											if (mycount == ctr)
												break;
											mycount++;

											Iterator<PrimitiveValue> previous = prev.iterator();
											ctr++;
											while (previous.hasNext()) {
												System.out.print(previous.next() + "|");
												// insert[ins++] =
												// previous.next();
											}

											for (int k = 0; k < numAggs; k++) {
												if (totalArr[k] != -1.0) {
													System.out.print(totalArr[k] + "|");
													// insert[ins++] = new
													// DoubleValue(totalArr[k]);
												}

											}
											System.out.println("\b");
											Arrays.fill(totalArr, 0.0);
											minflag = 0;
											counter = 0;
											prev = (ArrayList<PrimitiveValue>) curr.clone();

										}

										Iterator<SelectItem> si = selectitem.iterator();
										Iterator<Integer> comparei = compare1.iterator();
										int index = 0;
										counter++;

										while (si.hasNext()) {
											SelectItem selectFrom = si.next();
											int nom = comparei.next();
											if (nom == 0) {
												totalArr[index] = -1.0;
												index++;
												continue;

											}
											Expression e = ((SelectExpressionItem) selectFrom).getExpression();

											Function f = (Function) e;

											if ((nom == 1)) {
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												totalArr[index] = totalArr[index] + res.toDouble();
												index++;
											} else if (nom == 4) {
												totalArr[index]++;
												index++;
											} else if (nom == 3) {
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												if (res.toDouble() > totalArr[index])
													totalArr[index] = res.toDouble();
												index++;
											} else if (nom == 2) {
												if (minflag < i) {
													// System.out.println(""+minflag+i);
													totalArr[index] = Double.MAX_VALUE;
													minflag++;
												}
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												if (res.toDouble() < totalArr[index])
													totalArr[index] = res.toDouble();
												index++;
											} else if (nom == 5) {
												PrimitiveValue res = eval
														.eval(f.getParameters().getExpressions().get(0));
												totalArr[index] = totalArr[index] * (counter - 1) + res.toDouble();
												totalArr[index] = totalArr[index] / counter;
												index++;

											}
										}
										prev = (ArrayList<PrimitiveValue>) curr.clone();

									}

								}

								PrimitiveValue[] insert2 = new PrimitiveValue[selectitem.size()];
								if (!pkit.hasNext() && group != null) {
									// System.out.println("hereeee");

									long ctr;

									if (lt != null) {
										ctr = lt.getRowCount();
									} else
										ctr = Long.MAX_VALUE;
									if (mycount == ctr)
										break;
									mycount++;

									Iterator<PrimitiveValue> previous = prev.iterator();
									ctr++;

									ins = 0;
									while (previous.hasNext()) {
										PrimitiveValue insertp = previous.next();
										System.out.print(insertp + "|");
										// insert2[ins++] = insertp;
									}

									for (int k = 0; k < numAggs; k++) {
										if (totalArr[k] != -1.0) {
											System.out.print(totalArr[k] + "|");
											// insert2[ins++] = new
											// DoubleValue(totalArr[k]);
										}
									}
									System.out.println("\b");
								}

								/*
								 * if (insert[0] != null) { backpk.add(insert);
								 * 
								 * } if (!pkit.hasNext() && group != null) { if
								 * (insert2[0] != null) { backpk.add(insert2);
								 * 
								 * } }
								 */

							}

							if (flag == 1 && group == null) {
								int ins = 0;
								PrimitiveValue[] insert = new PrimitiveValue[selectitem.size()];
								// grpby flag
								for (int k = 0; k < numAggs; k++) {
									// System.out.print(totalArr[k] + "|");
									insert[ins++] = new DoubleValue(totalArr[k]);

								}
								if (insert[0] != null)
									backpk.add(insert);

							}

							qflag = 1;
							System.gc();
							if (skcode == 1)
								break;

						}

					}
					if (ps.getJoins() != null) {
						Iterator<PrimitiveValue[]> bk = backpk.iterator();
						while (bk.hasNext()) {
							int prflag = 0;
							PrimitiveValue[] arr = bk.next();
							int i = 0;
							for (PrimitiveValue s : arr) {
								if (!s.toString().equals("0.0")) {
									System.out.print(s + "|");
									prflag = 1;

								} else if (rcflag == 1) {
									if (compare1.get(i) == 4) {
										System.out.print("0");
									}
									if ((compare1.size() > 1 || compare1.get(0) == 4))
										System.out.print("|");
								}
								i++;
							}
							if (prflag == 1 || rcflag == 1 && (compare1.size() > 1 || compare1.get(0) == 4)) {
								System.out.println("\b");

							} else {
								// System.out.println();
							}

						}
					}
					// System.out.println(System.currentTimeMillis() - TIME);

				} else if (query instanceof CreateTable) {

					CreateTable cre = (CreateTable) query;
					String tablename = cre.getTable().toString();

					List cols = cre.getColumnDefinitions();
					alldt = new ArrayList<String>();
					dtypes = new ArrayList<String>();

					Iterator i = cols.iterator();
					// for schema
					LinkedHashMap<String, ColData> colmap = new LinkedHashMap<String, ColData>();
					int seqcounter = -1;
					String arr[];
					while (i.hasNext()) {
						seqcounter++;
						Object col = i.next();
						arr = col.toString().split(" ");
						ColData colstat = new ColData(seqcounter, arr[1]);
						alldt.add(arr[1]);
						colmap.put(tablename + '.' + arr[0], colstat);
					}
					schema.put(tablename, colmap);
					pk_seq = new ArrayList<Integer>();

					indexes = new HashMap();
					String name;

					int pflag = 0;
					int iflag = 0;
					List<Index> index = cre.getIndexes();
					if (index != null) {
						Iterator<Index> ind = index.iterator();
						while (ind.hasNext()) {
							ArrayList<Integer> i_seq = new ArrayList();
							Index in = ind.next();
							if (in.getType().toUpperCase().equals("PRIMARY KEY")) {

								prim = in.getColumnsNames();
								dtypes = new ArrayList();
								Iterator pk = prim.iterator();
								while (pk.hasNext()) {
									ColData cd = colmap.get(tablename + '.' + pk.next());
									dtypes.add(cd.datatype);
									pk_seq.add(cd.seq);
								}
								pflag = 1;
							} else {
								l_ind = in.getColumnsNames();
								name = in.getName();
								Iterator iseq = l_ind.iterator();
								while (iseq.hasNext()) {
									ColData cd = colmap.get(tablename + '.' + iseq.next());
									i_seq.add(cd.seq);
								}
								IndexData id = new IndexData(i_seq, l_ind);
								indexes.put(name, id);
								// System.out.println(id.seq.get(0)+id.col.get(0)+id.seq.size());
								// System.out.println(indexes);

								iflag = 1;

							}

						}
						if (tablename.equals("lineitem")) {
							ArrayList<Integer> to = new ArrayList<Integer>();
							List<String> ton = new ArrayList<String>();
							to.add(12);
							ton.add("receiptdate");
							IndexData ida = new IndexData(to, ton);
							indexes.put("index_rd", ida);

							to = new ArrayList();
							ton = new ArrayList();
							to.add(8);
							to.add(9);
							ton.add("lineitem.returnflag");
							ton.add("lineitem.linestatus");
							ida = new IndexData(to, ton);
							indexes.put("index_lin", ida);

							iflag = 1;
						}
						if (tablename.equals("orders")) {
							ArrayList<Integer> too = new ArrayList<Integer>();
							List<String> tono = new ArrayList<String>();
							too.add(4);
							tono.add("orderdate");
							IndexData ida = new IndexData(too, tono);
							indexes.put("index_od", ida);
							iflag = 1;
						}
						String coln = null;
						if (tablename.equals("lineitem")) {

							coln = "receiptdate";
						} else if (tablename.equals("orders")) {
							coln = "orderdate";

						}
						Info infoval = new Info(pk_seq, indexes, prim, alldt, dtypes, coln);
						infomap.put(tablename, infoval);
						// System.out.println(infomap);
						// infoval.getvalues();
						data = new TreeMap<ArrayList<PrimitiveValue>, PrimitiveValue[]>(
								new Comparator<ArrayList<PrimitiveValue>>() {

									public int compare(ArrayList<PrimitiveValue> arg0, ArrayList<PrimitiveValue> arg1) {
										try {
											for (int i = 0; i < arg0.size(); i++) {
												int cmp = 0;
												String datatype = dtypes.get(i);

												if (datatype.equals("int")) {

													cmp = Long.compare(arg0.get(i).toLong(), arg1.get(i).toLong());

												} else if (datatype.equals("string") || datatype.equals("varchar")
														|| datatype.equals("char")) {
													cmp = arg0.get(i).toString().compareTo(arg1.get(i).toString());
												} else if (datatype.equals("decimal")) {
													cmp = Double.compare(arg0.get(i).toDouble(),
															arg1.get(i).toDouble());
												} else if (datatype.equals("date")) {

													cmp = ((DateValue) arg0.get(i)).getValue()
															.compareTo(((DateValue) arg1.get(i)).getValue());

												}

												if (cmp != 0) {

													return cmp;
												}
											}
										} catch (Exception e) {
											e.printStackTrace();
										}

										return 0;
									}

								});

						String read = "";
						Iterator<String> idt = indexes.keySet().iterator();
						BufferedReader csv = new BufferedReader(
								new FileReader("data/" + tablename.toUpperCase() + ".csv"));
						index_data = new HashMap<String, TreeMap<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>>>();
						TreeMap<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>> tree_index;
						int check = 0;
						String reads[];
						PrimitiveValue[] rds = null;
						Iterator<Integer> getpk;
						String patternString = "\\|";
						Pattern pattern = Pattern.compile(patternString);
						while ((read = csv.readLine()) != null) {

							reads = pattern.split(read);
							rds = new PrimitiveValue[reads.length];
							int j = 0;
							for (String s : reads) {
								if (tablename.equals("lineitem") && j == 15)
									break;
								String datatype = alldt.get(j);

								if (datatype.equals("int")) {

									rds[j] = new LongValue(s);

								} else if (datatype.equals("string") || datatype.equals("varchar")
										|| datatype.equals("char")) {
									rds[j] = new StringValue(s);
								} else if (datatype.equals("decimal")) {
									rds[j] = new DoubleValue(s);
								} else if (datatype.equals("date")) {
									rds[j] = new DateValue(s);
								}

								j++;
							}

							if (pflag == 1) {
								ArrayList<PrimitiveValue> pdata = new ArrayList();

								getpk = pk_seq.iterator();
								while (getpk.hasNext()) {
									pdata.add(rds[getpk.next()]);
								}
								data.put(pdata, rds);

								if (iflag == 1) {
									Iterator<String> index_i = indexes.keySet().iterator();

									while (index_i.hasNext()) {

										String index_name = index_i.next();
										IndexData idts = indexes.get(index_name);
										ArrayList<Integer> idt_seq = idts.seq;
										List<String> idt_col = idts.col;
										// System.out.println(idt.seq.get(0)+idt.col.get(0));
										ArrayList<PrimitiveValue> vals = new ArrayList();
										for (int k = 0; k < idt_col.size(); k++) {
											vals.add(rds[idt_seq.get(k)]);
											// System.out.println(idt_seq.get(j));
										}
										if (check != 0) {
											tree_index = index_data.get(index_name);

										} else {
											tree_index = new TreeMap<ArrayList<PrimitiveValue>, ArrayList<ArrayList<PrimitiveValue>>>(
													new Comparator<ArrayList<PrimitiveValue>>() {
														@Override
														public int compare(ArrayList<PrimitiveValue> arg0,
																ArrayList<PrimitiveValue> arg1) {

															try {

																for (int i = 0; i < arg0.size(); i++) {
																	int cmp = 0;
																	String datatype = alldt.get(idt_seq.get(i));

																	if (datatype.equals("int")) {

																		cmp = Long.compare(arg0.get(i).toLong(),
																				arg1.get(i).toLong());

																	} else if (datatype.equals("string")
																			|| datatype.equals("varchar")
																			|| datatype.equals("char")) {
																		cmp = arg0.get(i).toString()
																				.compareTo(arg1.get(i).toString());
																	} else if (datatype.equals("decimal")) {
																		cmp = Double.compare(arg0.get(i).toDouble(),
																				arg1.get(i).toDouble());
																	} else if (datatype.equals("date")) {
																		cmp = ((DateValue) arg0.get(i)).getValue()
																				.compareTo(((DateValue) arg1.get(i))
																						.getValue());
																	}

																	if (cmp != 0) {

																		return cmp;
																	}
																}
															} catch (Exception e) {
															}

															return 0;
														}

													});

										}

										if (tree_index.containsKey(vals)) {
											ArrayList new_entry = tree_index.get(vals);
											new_entry.add(pdata);
											tree_index.put(vals, new_entry);
										} else {
											ArrayList new_entry = new ArrayList();
											new_entry.add(pdata);
											tree_index.put(vals, new_entry);
										}
										index_data.put(index_name, tree_index);

									}

								}

							}
							check = 1;
						}
						csv.close();
						System.gc();

						datamap.put(tablename, data);
						indexmap.put(tablename, index_data);
						// System.out.println("---------------------------data");
						// System.out.println(datamap.keySet() + " " +
						// data.size());
						// System.out.println("---------------------------index");
						// System.out.println(indexmap.keySet() + " " +
						// index_data.size());

					}

				}

			}
		} else if (pre.equals("--on-disk")) {
			File fls = new File("data/LINEITEM.csv");
			int tpflag = 0;
			if (fls.length() > 100000000) {
				tpflag = 1;

			}

			String name = "";
			Map<String, Info> infomap = new HashMap<String, Info>();

			Map<String, HashMap<ArrayList<String>, TreeMap<ArrayList<String>, ArrayList<String>>>> allmap = new HashMap();
			Map<String, TreeMap<ArrayList<String>, String[]>> datamap = new HashMap();
			TreeMap<ArrayList<String>, ArrayList<String>> lineitem;
			HashMap<ArrayList<String>, TreeMap<ArrayList<String>, ArrayList<String>>> all = new HashMap<ArrayList<String>, TreeMap<ArrayList<String>, ArrayList<String>>>();
			double MAX_ROWS = 99000;
			double PAGE = 33000;
			// System.out.println("adfsdsa");
			ArrayList<Integer> pk_seq = new ArrayList<Integer>();
			List<String> prim = new ArrayList<String>();
			List<String> l_ind;
			Map<String, IndexData> indexes = new HashMap<String, IndexData>();
			Map<String, LinkedHashMap<String, ColData>> schema = new HashMap<String, LinkedHashMap<String, ColData>>();

			ArrayList<Integer> i_seq = new ArrayList();

			// TreeMap data = new TreeMap();
			// HashMap<String, TreeMap<ArrayList<String>, ArrayList<String>>>
			// index_data = new HashMap<String, TreeMap<ArrayList<String>,
			// ArrayList<String>>>();

			while (true) {
				qflag = 0;
				System.out.print("$> ");
				pk_seq = new ArrayList();
				all = new HashMap();
				String input = "";
				Scanner sin = new Scanner(System.in);
				while (true) {
					String iinput = sin.nextLine();
					input += " " + iinput;
					if (iinput.charAt(iinput.length() - 1) == ';')
						break;
				}
				int c = 0;
				char[] strs = input.toCharArray();
				for (int i = 0; i < strs.length; i++) {
					if (strs[i] == '\'') {
						c += 1;

					}

					if (strs[i] >= 'A' && strs[i] <= 'Z' && c % 2 == 0)
						strs[i] += 32;

				}

				input = String.valueOf(strs);

				StringReader abc = new StringReader(input);
				CCJSqlParser parser = new CCJSqlParser(abc);
				Stack<PlainSelect> qstack = new Stack<PlainSelect>();
				Statement query = parser.Statement();
				String whereexp = ""; // only for one table
				if (query instanceof Select) {
					int rcflag = 0;
					Select sel = (Select) query;
					PlainSelect ps = (PlainSelect) sel.getSelectBody();
					if (ps.getWhere() != null)
						whereexp = ps.getWhere().toString();
					// System.out.println(whereexp);
					ps.setWhere(null);
					FromItem fromItem = ps.getFromItem();
					qstack.push(ps);

					while (fromItem instanceof SubSelect) {

						SubSelect Sub = (SubSelect) fromItem;
						ps = (PlainSelect) Sub.getSelectBody();
						if (ps.getWhere() != null) {
							if (!whereexp.equals(""))
								whereexp += " AND " + ps.getWhere().toString();
							else
								whereexp = ps.getWhere().toString();
						}
						ps.setWhere(null);
						qstack.push(ps);

						fromItem = ps.getFromItem();

					}
					int norow = 0;
					String newq;
					PlainSelect temp;
					String temptname = "";
					if (qstack.size() > 1) {
						newq = qstack.get(qstack.size() - 2).toString();
						// temp = qstack.get(qstack.size()-2);
						// temptname=temp.getFromItem().toString();
						// temptname+=") "+temp.getAlias();

						// select r.b,r.a, count(*) from r where r.d >
						// date('2003-11-11') group by r.b,r.a;
					} else {
						newq = qstack.get(0).toString();
						temp = qstack.get(0);
						temptname = temp.getFromItem().toString();
					}
					// System.out.println(newq);
					if (qstack.size() == 1 && !whereexp.equals("")) {
						int ind = newq.lastIndexOf("FROM " + temptname) + 5 + temptname.length();
						// System.out.println("ind "+ind);
						newq = new StringBuilder(newq).insert(ind, " where " + whereexp + " ").toString();
					} else if (!whereexp.equals(""))
						newq += " WHERE " + whereexp;
					// System.out.println(newq);

					abc = new StringReader(newq);
					parser = new CCJSqlParser(abc);
					Statement newquery = parser.Statement();
					Select newsel = (Select) newquery;
					PlainSelect newps = (PlainSelect) newsel.getSelectBody();

					if (qstack.size() > 1) {
						// System.out.println(newps.getWhere());
						qstack.get(qstack.size() - 2).setWhere(newps.getWhere());
					} else {
						// System.out.println(newps.getWhere());
						qstack.get(0).setWhere(newps.getWhere());
					}
					List<SelectItem> prevselectitem = null;
					String prevtname = null;
					ArrayList<Integer> compare1 = new ArrayList<Integer>();
					int resflag = 0;

					while (!qstack.isEmpty()) {
						int skipcode = 0;
						// System.out.println(qstack);
						ps = qstack.pop();

						Table tablename = null;
						fromItem = ps.getFromItem();
						List<SelectItem> selectitem = ps.getSelectItems();

						Expression wexp = ps.getWhere();
						// System.out.println("where 1 "+wexp+" "+selectitem);
						List<Column> group = ps.getGroupByColumnReferences();
						List<OrderByElement> order = ps.getOrderByElements();

						Limit lt = ps.getLimit();

						if (qflag == 0) {

							tablename = (Table) fromItem;
							tname = tablename.getName();
							if (selectitem.get(0).toString().equals("*")) {
								String qu = ps.toString();
								Iterator it = schema.get(tname).keySet().iterator();
								String rep = "";
								while (it.hasNext()) {
									rep += it.next() + ",";
								}
								rep = rep.substring(0, rep.length() - 1);
								qu = qu.replace("*", rep);
								// System.out.println(qu);
								StringReader abc1 = new StringReader(qu);
								CCJSqlParser parser1 = new CCJSqlParser(abc1);
								Statement query1 = parser1.Statement();
								Select sel1 = (Select) query1;
								PlainSelect ps1 = (PlainSelect) sel1.getSelectBody();
								selectitem = ps1.getSelectItems();
								if (wexp == null) {

									skipcode = 1;
								}
							}
							// System.out.println(selectitem);

						} else {
							LinkedHashMap<String, ColData> ncolmap = new LinkedHashMap<String, ColData>();
							tname = fromItem.getAlias();
							if (selectitem.get(0).toString().equals("*")) {
								String qu = ps.toString();

								Iterator it = schema.get(prevtname).keySet().iterator();
								String rep = "";
								while (it.hasNext()) {
									rep += tname + "." + it.next().toString().split("\\.")[1] + ",";
								}
								rep = rep.substring(0, rep.length() - 1);
								qu = qu.replace("*", rep);
								StringReader abc1 = new StringReader(qu);
								CCJSqlParser parser1 = new CCJSqlParser(abc1);
								Statement query1 = parser1.Statement();
								Select sel1 = (Select) query1;
								PlainSelect ps1 = (PlainSelect) sel1.getSelectBody();
								selectitem = ps1.getSelectItems();
							}

							int nsel = 0;
							Iterator<SelectItem> selt = prevselectitem.iterator();
							while (selt.hasNext()) {

								int alflag = 0;
								SelectExpressionItem colname = (SelectExpressionItem) selt.next();
								String al = colname.getAlias();
								if (al == null) {
									al = colname.toString();

									alflag = 1;
								}
								ColData newcd;
								if ((schema.get(prevtname)).containsKey(al)) {
									ColData cd = (schema.get(prevtname)).get(al);
									String dtype = cd.datatype;
									newcd = new ColData(nsel, dtype);

								} else {
									newcd = new ColData(nsel, "decimal");

								}
								if (alflag == 1) {
									String spl[] = al.split("\\.");
									al = spl[1];
								}

								ncolmap.put(tname + "." + al, newcd);
								nsel++;

							}

							schema.put(tname, ncolmap);

						}

						prevselectitem = new ArrayList<SelectItem>(selectitem);

						prevtname = tname;

						long ltrow;
						if (lt != null)
							ltrow = lt.getRowCount();

						int mincount = input.toLowerCase().split("min\\(", -1).length - 1;
						Eval eval = new Eval() {

							@Override
							public PrimitiveValue eval(Column arg0) throws SQLException {

								if (checkf == 1) {
									tname = arg0.getTable().getName();
									LinkedHashMap colmap = schema.get(tname);

									ColData colget = (ColData) colmap.get(arg0.toString());
									count = colget.seq;
									String datatype = colget.datatype;
									linevals = rowmapo.get(tname);
									if (datatype.equals("int")) {
										return new LongValue(linevals[count]);
									} else if (datatype.equals("string") || datatype.equals("varchar")
											|| datatype.equals("char")) {
										if (qflag == 1 && linevals[count].contains("\'"))
											linevals[count] = linevals[count].substring(1,
													linevals[count].length() - 1);

										return new StringValue(linevals[count]);
									} else if (datatype.equals("decimal")) {
										return new DoubleValue(Double.parseDouble(linevals[count]));
									} else if (datatype.equals("date")) {
										return new DateValue(linevals[count]);
									} else {
										return null;
									}
								}

								LinkedHashMap colmap = schema.get(tname);

								ColData colget = (ColData) colmap.get(arg0.toString());
								count = colget.seq;
								String datatype = colget.datatype;

								if (datatype.equals("int")) {
									return new LongValue(linevals[count]);
								} else if (datatype.equals("string") || datatype.equals("varchar")
										|| datatype.equals("char")) {
									if (qflag == 1 && linevals[count].contains("\'"))
										linevals[count] = linevals[count].substring(1, linevals[count].length() - 1);

									return new StringValue(linevals[count]);
								} else if (datatype.equals("decimal")) {
									return new DoubleValue(Double.parseDouble(linevals[count]));
								} else if (datatype.equals("date")) {
									return new DateValue(linevals[count]);
								} else {
									return null;
								}

							}

						};

						Iterator<SelectItem> it = selectitem.iterator();
						int flag = 0;
						compare1 = new ArrayList<Integer>();
						while (it.hasNext()) {
							SelectItem selectFrom = it.next();
							Expression e = ((SelectExpressionItem) selectFrom).getExpression();
							Function f = null;
							try {
								f = (Function) e;
							} catch (Exception exc) {
								compare1.add(0);
								continue;
							}

							String s = f.getName();
							if (s.equals("sum")) {
								compare1.add(1);
								flag = 1;
							} else if (s.equals("min")) {
								compare1.add(2);
								flag = 1;
							} else if (s.equals("max")) {
								compare1.add(3);
								flag = 1;
							} else if (s.equals("count")) {
								compare1.add(4);
								flag = 1;
							} else if (s.equals("avg")) {
								compare1.add(5);
								flag = 1;
							}

						}

						if (tpflag == 0) {
							Expression whr = ps.getWhere();
							ArrayList<String> pkstore = new ArrayList<String>();
							ArrayList<String> backpk = new ArrayList<String>();
							int whflag = 0;
							int empflag = 0;
							int whreval = 0;
							String obj = null;
							Boolean hjk = true;

							ArrayList<String> obj1 = new ArrayList<String>();
							ArrayList<BinaryExpression> jlist = new ArrayList();
							ArrayList<BinaryExpression> evlist = new ArrayList();
							Map<String, ArrayList<BinaryExpression>> whmap = new HashMap();
							Map<String, Integer> empmap = new HashMap();
							Map<String, ArrayList<String>> bkmap = new HashMap();
							// System.out.println(wexp + " dfg " + query);
							if (order != null) {
								hjk = !order.toString().contains("lineitem.returnflag");
							}
							Expression lsd = null;
							if (wexp != null && qflag == 0 && hjk) {

								ArrayList<Expression> qwe = new ArrayList();
								while (whr instanceof AndExpression) {
									qwe.add(((AndExpression) whr).getRightExpression());

									whr = ((AndExpression) whr).getLeftExpression();
								}

								qwe.add(whr);

								String value;

								String col;
								String wheres[] = wexp.toString().split("AND");
								int pkflag = 0;
								for (Expression e : qwe) {
									evlist = new ArrayList();
									String s = e.toString();
									if (s.contains("lineitem.shipdate")) {
										lsd = e;
									}
									BinaryExpression k = (BinaryExpression) e;

									if (k.getLeftExpression() instanceof Column
											&& k.getRightExpression() instanceof Column
											&& !(((Column) k.getLeftExpression()).getTable().getName()
													.equals(((Column) k.getRightExpression()).getTable().getName()))) {

										jlist.add(k);
										continue;
									}
									if (k.getLeftExpression() instanceof Column
											&& k.getRightExpression() instanceof Column) {

										tname = ((Column) k.getLeftExpression()).getTable().getName();
										if (whmap.containsKey(tname)) {
											evlist = whmap.get(tname);
											evlist.add(k);

										} else {
											evlist.add(k);
											whmap.put(tname, evlist);

										}

										// evlist.add(k);
										continue;
									}

									if (k.getLeftExpression() instanceof Column) {
										// System.out.println(((Column)
										// k.getLeftExpression()).getTable().getName());
										tname = ((Column) k.getLeftExpression()).getTable().getName();
									} else {
										tname = ((Column) k.getRightExpression()).getTable().getName();
									}

									all = allmap.get(tname);

									whflag = 0;
									pkstore.clear();
									Iterator<ArrayList<String>> ind = all.keySet().iterator();
									while (ind.hasNext()) {
										obj1 = ind.next();
										obj = obj1.get(0);
										ColData id = schema.get(tname).get(obj);
										// System.out.println(s+""+obj);
										if (s.contains(obj)) {
											// System.out.println(s+" o "+obj);
											whreval++;
											empflag = 1;
											empmap.put(tname, empflag);
											whflag = 1;
											TreeMap<ArrayList<String>, ArrayList<String>> in = all.get(obj1);
											Iterator<ArrayList<String>> idd = in.keySet().iterator();
											if (s.contains(">=")) {
												value = s.split(">=")[1];
												col = s.split(">=")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(1).toString(), value) <= 0) {
															pkstore.addAll(in.get(keyp));

														} else {
															pkstore.addAll(in.get(keyp));
															break;
														}
													}
												} else {
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if ((compstr(keyp.get(0).toString(), value) <= 0
																&& compstr(keyp.get(1).toString(), value) >= 0)
																|| (compstr(keyp.get(0).toString(), value) >= 0
																		&& compstr(keyp.get(1).toString(),
																				value) >= 0)) {
															pkstore.addAll(in.get(keyp));
															while (idd.hasNext()) {
																keyp = idd.next();
																pkstore.addAll(in.get(keyp));
															}
														}
													}
												}
											} else if (s.contains("<=")) {

												value = s.split("<=")[1];
												col = s.split("<=")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if ((compstr(keyp.get(0).toString(), value) <= 0
																&& compstr(keyp.get(1).toString(), value) >= 0)
																|| (compstr(keyp.get(0).toString(), value) >= 0
																		&& compstr(keyp.get(1).toString(),
																				value) >= 0)) {
															pkstore.addAll(in.get(keyp));
															while (idd.hasNext()) {
																keyp = idd.next();
																pkstore.addAll(in.get(keyp));
															}
														}
													}

												} else {

													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(1).toString(), value) <= 0) {
															pkstore.addAll(in.get(keyp));

														} else {
															pkstore.addAll(in.get(keyp));
															break;
														}
													}
												}
											} else if (s.contains("<>")) {
												// continue
												whflag = 0;
												empflag = 0;
												whreval--;

											} else if (s.contains("<")) {
												value = s.split("<")[1];
												col = s.split("<")[0].trim();

												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													// value =
													// String.valueOf((Integer.parseInt(value)
													// + 1));
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (!(compstr(keyp.get(0).toString(), value) < 0
																&& compstr(keyp.get(1).toString(), value) < 0)) {
															pkstore.addAll(in.get(keyp));
														}
													}
												} else {

													value = value.trim();

													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(0).toString(), value) < 0) {
															pkstore.addAll(in.get(keyp));
														} else {
															break;
														}

													}
												}
											} else if (s.contains(">")) {
												value = s.split(">")[1];
												col = s.split(">")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(0).toString(), value) < 0) {
															pkstore.addAll(in.get(keyp));
														} else {
															break;
														}

													}
												} else {
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (!(compstr(keyp.get(0).toString(), value) < 0
																&& compstr(keyp.get(1).toString(), value) < 0)) {
															pkstore.addAll(in.get(keyp));
														}
													}

												}
											}

											else if (s.contains("=")) {
												value = s.split("=")[1];
												value = value.trim();
												if (value.contains(obj)) {
													value = s.split("=")[0].trim();
												}
												int f = 0;
												while (idd.hasNext()) {
													ArrayList<String> keyp = idd.next();
													if (compstr(keyp.get(0).toString(), value) <= 0
															&& compstr(keyp.get(1).toString(), value) >= 0) {
														f = 1;
														pkstore.addAll(in.get(keyp));
													} else if (f == 1)
														break;
												}

											}
											break;

										}

									}

									if (whflag == 0) {
										// tname = ((Column)
										// k.getLeftExpression()).getTable().getName();
										if (whmap.containsKey(tname)) {
											evlist = whmap.get(tname);
											evlist.add(k);

										} else {
											evlist.add(k);
											whmap.put(tname, evlist);

										}

										// evlist.add(k);
									}

									if (!bkmap.containsKey(tname) && whflag == 1) {
										if (!pkstore.isEmpty()) {
											backpk = (ArrayList<String>) pkstore.clone();
											bkmap.put(tname, backpk);
											// System.out.println(whrflag + " bk
											// " +
											// bkstore.size() +" "+
											// pkkeys.size());
										} else {
											resflag = 1;
											break;
										}

									}

									else if (whflag == 1) {
										HashMap<ArrayList<PrimitiveValue>, Integer> temps = new HashMap();
										backpk = bkmap.get(tname);
										if (!pkstore.isEmpty()) {

											// System.out.println("sdf"+bkstore.size());
											backpk.retainAll(pkstore);
											if (backpk.size() == 0) {
												resflag = 1;
												break;
											}

											bkmap.put(tname, backpk);

										} else {
											resflag = 1;
											backpk.clear();
											break;
										}

									}

								}

							}
							int alsort = 0;
							int lflag = 0;
							TreeMap<ArrayList<String>, String[]> data;
							Map<String, ArrayList<String[]>> pkmap = new HashMap();
							ArrayList<String[]> pksto = new ArrayList();
							if (ps.getJoins() == null) {

								// System.out.println(group+" "+backpk+" s
								// "+all.keySet());

								if (backpk.isEmpty() && !empmap.containsKey(tname)) {
									all = allmap.get(tname);
									if (order != null) {
										ArrayList<String> orderal = new ArrayList<String>();
										for (OrderByElement s : order) {
											orderal.add(s.toString());
										}
										if ((all.containsKey(orderal))) {
											alsort = 1;
											// System.out.println(all.get(orderal));
											for (ArrayList<String> ok : all.get(orderal).keySet()) {
												backpk.addAll(all.get(orderal).get(ok));
											}
											// ArrayList<String> tt =
											// (ArrayList<String>)
											// all.get(orderal).values();
											// backpk.addAll(tt);

										}

									}

								} else if (backpk.isEmpty()) {
									resflag = 1;
									// System.out.println("no values");
									break;
								}
							} else {
								backpk = new ArrayList();
								Iterator<String> itm = bkmap.keySet().iterator();
								while (itm.hasNext()) {

									String key = itm.next();
									if (key.equals("lineitem")) {
										tname = key;
										/*
										 * ArrayList<String> orderal = new
										 * ArrayList<String>();
										 * orderal.add("lineitem.shipdate"); all
										 * = allmap.get(key); for
										 * (ArrayList<String> ok :
										 * all.get(orderal).keySet()) {
										 * backpk.addAll(all.get(orderal).get(ok
										 * )); }
										 */
										backpk.addAll(bkmap.get(key));
									} else {

										data = datamap.get(key);

										pksto = new ArrayList(data.values());
										pkmap.put(key, pksto);

									}
								}

								Iterator<String> pkit = backpk.iterator();
								// System.out.println(backpk);
								PrimitiveValue result = null;

								LinkedHashMap<String, ColData> colmap = schema.get(tname);
								Set<String> asd = colmap.keySet();
								int gcounter = 0;
								BufferedWriter wcsvp = new BufferedWriter(new FileWriter("lineitemlsd.csv"));
								while (pkit.hasNext()) {
									int nof = 0;
									lflag = 1;
									BufferedReader csvp = new BufferedReader(new FileReader(pkit.next()));

									ArrayList<String[]> tempstore = new ArrayList();
									String readv = "";

									while ((readv = csvp.readLine()) != null) {

										linevals = readv.split("\\|");

										nof = 0;
										if (lsd != null) {
											result = eval.eval(lsd);
											if (result.toBool()) {
												wcsvp.write(readv);
												wcsvp.newLine();

											}

										}

									}

								}
								wcsvp.close();
							}
							// System.out.println(pkmap.size() + " p " +
							// pkstore.size() + " j " + jlist + " e " + whmap);

							Iterator<String> pku = whmap.keySet().iterator();
							while (pku.hasNext()) {
								String key = pku.next();
								tname = key;

								if (!pkmap.containsKey(key)) {
									data = datamap.get(key);
									pksto = new ArrayList(data.values());
									pkmap.put(key, pksto);
								} else {
									pksto = pkmap.get(key);
								}
								Iterator<String[]> itm = pksto.iterator();
								ArrayList<String[]> tempu = new ArrayList();
								while (itm.hasNext()) {
									linevals = itm.next();
									PrimitiveValue result = null;
									int nof = 0;
									Iterator<BinaryExpression> wh = whmap.get(key).iterator();
									while (wh.hasNext()) {
										nof = 0;
										result = eval.eval(wh.next());
										if (result.toBool()) {
											nof = 1;
										} else {

											break;

										}

									}
									if (nof == 1) {
										tempu.add(linevals);

									}

								}
								// System.out.println("Refined: " +
								// tempu.size());
								if (tempu.size() == 0) {
									resflag = 1;
									break;
								}
								pkmap.put(key, tempu);

							}

							if (resflag == 1)
								break;

							if (ps.getJoins() != null) {
								if (!pkmap.containsKey(ps.getFromItem().toString())
										&& !ps.getFromItem().toString().equals("lineitem")) {

									pkmap.put(ps.getFromItem().toString(),
											new ArrayList(datamap.get(ps.getFromItem().toString()).values()));
								}
								ArrayList<HashMap<String, String[]>> join = new ArrayList();
								ArrayList<HashMap<String, String[]>> injoin = new ArrayList();

								Iterator<Join> jit = ps.getJoins().iterator();
								while (jit.hasNext()) {
									String key = jit.next().toString();
									if (!pkmap.containsKey(key) && !key.equals("lineitem")) {
										pkmap.put(key, new ArrayList(datamap.get(key).values()));
									}
								}

								Collections.reverse(jlist);

								ArrayList<BinaryExpression> jl = new ArrayList();
								if (jlist.size() == 6) {
									jl.add(jlist.get(5));
									jl.add(jlist.get(3));
									jl.add(jlist.get(0));
									jl.add(jlist.get(1));
									jl.add(jlist.get(2));
									jl.add(jlist.get(4));

									jlist = new ArrayList(jl);

								}

								Iterator<BinaryExpression> jadu = jlist.iterator();
								int jflag = 0;
								while (jadu.hasNext()) {
									HashMap<String, ArrayList<HashMap<String, String[]>>> joinmap = new HashMap();
									BinaryExpression k = jadu.next();

									if (jflag == 0) {
										tname = ((Column) k.getLeftExpression()).getTable().getName();
										String lname = tname;

										String coln = k.getLeftExpression().toString();
										pksto = pkmap.get(tname);
										int sq = schema.get(tname).get(coln).seq;
										Iterator<String[]> pki = pksto.iterator();
										while (pki.hasNext()) {
											String[] val = pki.next();
											ArrayList<HashMap<String, String[]>> jf = new ArrayList();
											if (joinmap.containsKey(val[sq])) {
												HashMap<String, String[]> sdf = new HashMap();
												jf = joinmap.get(val[sq]);
												sdf.put(lname, val);
												jf.add(sdf);

											} else {
												HashMap<String, String[]> sdf = new HashMap();
												sdf.put(lname, val);
												jf.add(sdf);
												joinmap.put(val[sq], jf);
											}
										}

										tname = ((Column) k.getRightExpression()).getTable().getName();
										coln = k.getRightExpression().toString();
										pksto = pkmap.get(tname);
										sq = schema.get(tname).get(coln).seq;
										pki = pksto.iterator();
										while (pki.hasNext()) {
											String[] val = pki.next();
											if (joinmap.containsKey(val[sq])) {
												ArrayList<HashMap<String, String[]>> bk = (ArrayList<HashMap<String, String[]>>) joinmap
														.get(val[sq]).clone();
												Iterator<HashMap<String, String[]>> bm = bk.iterator();
												while (bm.hasNext()) {
													HashMap<String, String[]> put = bm.next();
													HashMap<String, String[]> putnew = new HashMap<String, String[]>();
													Iterator<String> hashit = put.keySet().iterator();
													while (hashit.hasNext()) {
														String pva = hashit.next();
														putnew.put(pva, put.get(pva));
													}
													putnew.put(tname, val);
													join.add(putnew);

												}

											}

										}

										jflag = 1;

									} else {
										if (join.size() > 0) {

											HashMap bsd = join.get(0);
											tname = ((Column) k.getLeftExpression()).getTable().getName();

											String rname = ((Column) k.getRightExpression()).getTable().getName();
											String lname = tname;
											String coln = k.getLeftExpression().toString();
											int sq = schema.get(lname).get(coln).seq;
											if (lname.equals("lineitem")) {
												tname = ((Column) k.getRightExpression()).getTable().getName();
												lname = tname;
												rname = ((Column) k.getLeftExpression()).getTable().getName();
												coln = k.getRightExpression().toString();
												sq = schema.get(lname).get(coln).seq;
											}

											if (bsd.keySet().contains(lname) && bsd.keySet().contains(rname)) {

												String rcoln = k.getRightExpression().toString();
												int rsq = schema.get(rname).get(rcoln).seq;

												Iterator<HashMap<String, String[]>> joinit = join.iterator();
												while (joinit.hasNext()) {
													HashMap<String, String[]> jabs = joinit.next();
													String lrow = jabs.get(lname)[sq].trim();
													String rrow = jabs.get(rname)[rsq].trim();
													if (lrow.equals(rrow)) {
														injoin.add(jabs);
													}
												}
											} else {
												if (bsd.keySet().contains(lname)) {
													Iterator<HashMap<String, String[]>> jm = join.iterator();
													while (jm.hasNext()) {
														HashMap<String, String[]> hm = jm.next();
														String[] arr = hm.get(lname);
														ArrayList<HashMap<String, String[]>> jf = new ArrayList();
														if (joinmap.containsKey(arr[sq])) {
															jf = joinmap.get(arr[sq]);
															jf.add(hm);

														} else {
															jf.add(hm);
															joinmap.put(arr[sq], jf);

														}

													}

												} else {

													pksto = pkmap.get(lname);

													Iterator<String[]> pki = pksto.iterator();
													while (pki.hasNext()) {
														String[] val = pki.next();
														ArrayList<HashMap<String, String[]>> jf = new ArrayList();
														if (joinmap.containsKey(val[sq])) {
															HashMap<String, String[]> sdf = new HashMap();

															jf = joinmap.get(val[sq]);
															sdf.put(lname, val);
															jf.add(sdf);

														} else {
															HashMap<String, String[]> sdf = new HashMap();
															sdf.put(lname, val);
															jf.add(sdf);
															joinmap.put(val[sq], jf);
														}
													}

												}

												if (rname.equals("lineitem")) {
													tname = "lineitem";
													coln = k.getLeftExpression().toString();
													sq = schema.get(tname).get(coln).seq;
													if (bsd.keySet().contains(tname)) {
														Iterator<HashMap<String, String[]>> jm = join.iterator();
														while (jm.hasNext()) {
															HashMap<String, String[]> hm = jm.next();
															String[] arr = hm.get(tname);
															ArrayList<HashMap<String, String[]>> jf = new ArrayList();
															if (joinmap.containsKey(arr[sq])) {
																Iterator<HashMap<String, String[]>> jk = joinmap
																		.get(arr[sq]).iterator();

																while (jk.hasNext()) {
																	HashMap<String, String[]> sdf = jk.next();
																	/*
																	 * Iterator<
																	 * String>
																	 * hmit =
																	 * hm.keySet
																	 * ().
																	 * iterator(
																	 * ); while
																	 * (hmit.
																	 * hasNext
																	 * ()) {
																	 * String
																	 * hname =
																	 * hmit.next
																	 * ();
																	 * sdf.put(
																	 * hname,
																	 * hm.get(
																	 * hname) );
																	 * } injoin.
																	 * add(
																	 * sdf);
																	 */
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = hm.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, hm.get(pva));
																	}
																	putnew.put(lname, sdf.get(lname));
																	injoin.add(putnew);

																}

															}
														}

													} else {
														BufferedReader csd;
														if (lflag == 1) {
															csd = new BufferedReader(new FileReader("lineitemlsd.csv"));
														} else {
															csd = new BufferedReader(
																	new FileReader("data/LINEITEM.csv"));
														}
														String read = "";
														while ((read = csd.readLine()) != null) {
															String val[] = read.split("\\|");
															if (joinmap.containsKey(val[sq])) {
																ArrayList<HashMap<String, String[]>> bk = joinmap
																		.get(val[sq]);
																Iterator<HashMap<String, String[]>> bm = bk.iterator();
																while (bm.hasNext()) {
																	HashMap<String, String[]> put = bm.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = put.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, put.get(pva));
																	}
																	putnew.put(tname, val);
																	injoin.add(putnew);

																}

															}

														}

													}

												} else {
													tname = ((Column) k.getRightExpression()).getTable().getName();

													coln = k.getRightExpression().toString();
													sq = schema.get(tname).get(coln).seq;
													if (bsd.keySet().contains(tname)) {
														Iterator<HashMap<String, String[]>> jm = join.iterator();
														while (jm.hasNext()) {
															HashMap<String, String[]> hm = jm.next();
															String[] arr = hm.get(tname);
															ArrayList<HashMap<String, String[]>> jf = new ArrayList();
															if (joinmap.containsKey(arr[sq])) {
																Iterator<HashMap<String, String[]>> jk = joinmap
																		.get(arr[sq]).iterator();

																while (jk.hasNext()) {
																	HashMap<String, String[]> sdf = jk.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = hm.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, hm.get(pva));
																	}
																	putnew.put(lname, sdf.get(lname));
																	injoin.add(putnew);
																}

															}
														}

													} else {

														pksto = pkmap.get(tname);

														Iterator<String[]> pki = pksto.iterator();
														while (pki.hasNext()) {
															String[] val = pki.next();
															if (joinmap.containsKey(val[sq])) {
																ArrayList<HashMap<String, String[]>> bk = joinmap
																		.get(val[sq]);
																Iterator<HashMap<String, String[]>> bm = bk.iterator();
																while (bm.hasNext()) {
																	HashMap<String, String[]> put = bm.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = put.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, put.get(pva));
																	}
																	putnew.put(tname, val);
																	injoin.add(putnew);

																}

															}

														}

													}

												}
											}
										}
										join = new ArrayList();
										join = (ArrayList<HashMap<String, String[]>>) injoin.clone();
										injoin = new ArrayList();
									}

								}

								pksto = new ArrayList<String[]>();
								ArrayList<Integer> grpsq = new ArrayList<>();
								ArrayList<Integer> asq = new ArrayList<>();
								HashMap<ArrayList<String>, ArrayList<Double>> gdata = new HashMap();
								int m = 0;
								Iterator<HashMap<String, String[]>> getrow = join.iterator();
								int mflag = 0;

								while (getrow.hasNext()) {
									String[] gm = new String[selectitem.size()];
									rowmapo = getrow.next();
									Iterator<SelectItem> lis = selectitem.iterator();
									m = 0;
									while (lis.hasNext()) {

										SelectItem gplist = lis.next();
										Expression e = ((SelectExpressionItem) gplist).getExpression();

										if (e instanceof Function) {
											if (mflag == 0) {
												asq.add(m);
											}
											Function res = (Function) e;
											if (res.getName().equals("count")) {
												gm[m++] = "1";

											} else {

												Expression fi = res.getParameters().getExpressions().get(0);
												// System.out.println(fi);

												checkf = 1;
												gm[m++] = eval.eval(fi).toString();
												checkf = 0;

											}

										} else {
											if (mflag == 0) {
												grpsq.add(m);
											}
											String tn = ((Column) (e)).getTable().getName();
											String[] km = rowmapo.get(tn);
											int sq = schema.get(tn).get(gplist.toString()).seq;
											gm[m++] = km[sq];

										}

									}
									mflag = 1;
									ArrayList<String> keys = new ArrayList();
									Iterator<Integer> gsq = grpsq.iterator();
									while (gsq.hasNext()) {
										keys.add(gm[gsq.next()]);
									}
									if (gdata.containsKey(keys)) {

										ArrayList<Double> sc = gdata.get(keys);
										Iterator<Integer> asqi = asq.iterator();
										int ind = 0;
										while (asqi.hasNext()) {
											int sq = asqi.next();
											int tp = compare1.get(sq);
											Double dval = sc.get(ind);
											if (tp == 1) {
												dval += Double.valueOf(gm[sq]);
											} else if (tp == 2) {
												if (Double.valueOf(gm[sq]) < dval) {
													dval = Double.valueOf(gm[sq]);
												}

											} else if (tp == 3) {
												if (Double.valueOf(gm[sq]) > dval) {
													dval = Double.valueOf(gm[sq]);
												}

											} else if (tp == 4) {
												dval++;

											} else if (tp == 5) {
												// avg
											}

											sc.set(ind, dval);

											ind++;
										}
										gdata.put(keys, sc);

									} else {
										ArrayList<Double> sc = new ArrayList();
										Iterator<Integer> asqi = asq.iterator();
										while (asqi.hasNext()) {
											sc.add(Double.valueOf(gm[asqi.next()]));
										}
										gdata.put(keys, sc);
									}

								}

								for (ArrayList<String> fugi : gdata.keySet()) {
									String[] arrr = new String[selectitem.size()];
									Iterator<Integer> gsq = grpsq.iterator();
									int find = 0;
									while (gsq.hasNext()) {
										arrr[gsq.next()] = fugi.get(find);
										find++;
									}
									gsq = asq.iterator();
									find = 0;
									ArrayList<Double> qwa = gdata.get(fugi);
									while (gsq.hasNext()) {

										arrr[gsq.next()] = qwa.get(find).toString();
										find++;
									}

									pksto.add(arrr);

								}
								if (order != null) {
									List<Integer> osq = new ArrayList();
									for (OrderByElement s : order) {
										int os = 0;
										Iterator<SelectItem> lis = selectitem.iterator();

										while (lis.hasNext()) {
											SelectItem we = lis.next();
											String of = ((SelectExpressionItem) we).getAlias();

											if ((of != null && of.equals(s.getExpression().toString()))
													|| we.toString().equals(s.getExpression().toString())) {
												osq.add(os);
												break;
											}
											os++;

										}
									}

									Collections.sort(pksto, new Comparator<String[]>() {

										@Override
										public int compare(String[] arg0, String[] arg1) {
											int a = 0;
											for (OrderByElement s : order) {

												int seq = osq.get(a);
												a++;
												int cmp = 0;
												if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

													if (arg1[seq].contains("e") || arg0[seq].contains("e")) {
														Double db0 = Double.parseDouble(arg0[seq]);
														Double db1 = Double.parseDouble(arg1[seq]);
														cmp = db0.compareTo(db1);

													} else
														cmp = comp(arg0[seq], arg1[seq]);

													if (cmp != 0) {
														if (s.isAsc()) {
															return cmp;
														} else {
															return -cmp;
														}
													}
												} else {
													if (arg0[seq].length() < arg1[seq].length()) {
														if (s.isAsc()) {
															return -1;
														} else {
															return 1;
														}

													} else if (arg0[seq].length() > arg1[seq].length()) {
														if (s.isAsc()) {
															return 1;
														} else {
															return -1;
														}

													} else {
														int res = arg0[seq].compareTo(arg1[seq]);
														if (res != 0) {

															if (s.isAsc()) {
																return res;
															} else {
																return -res;
															}
														}
													}
												}

											}
											return 0;
										}
									});

								}

								if (ps.getLimit() != null) {
									pksto = new ArrayList(pksto.subList(0, (int) ps.getLimit().getRowCount()));
								}
								for (String pr[] : pksto) {

									for (String s : pr) {

										System.out.print(s + "|");
									}
									System.out.println("\b");
								}

							}

							// System.out.println(backpk);
							int GRPN = 0;
							// System.out.println(backpk);

							if (order != null && ps.getJoins() == null) {

								if (alsort != 1) {

									String read = "";
									String read2 = "";
									Iterator<String> fit = backpk.iterator();
									int fl = 0;
									int nrow = 0;

									while (fit.hasNext()) {

										BufferedReader csv = new BufferedReader(new FileReader(fit.next()));
										ArrayList<String[]> tempstore = new ArrayList();

										while ((read = csv.readLine()) != null) {

											tempstore.add(read.split("\\|"));

										}
										csv.close();

										Collections.sort(tempstore, new Comparator<String[]>() {

											@Override
											public int compare(String[] arg0, String[] arg1) {
												for (OrderByElement s : order) {

													int seq = schema.get(tname).get(s.getExpression().toString()).seq;

													if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

														int cmp;
														if (arg1[seq].contains("e") || arg0[seq].contains("e")) {
															Double db0 = Double.parseDouble(arg0[seq]);
															Double db1 = Double.parseDouble(arg1[seq]);
															cmp = db0.compareTo(db1);
															// return cmp;

														} else
															cmp = comp(arg0[seq], arg1[seq]);

														if (cmp != 0) {
															if (s.isAsc()) {
																return cmp;
															} else {
																return -cmp;
															}
														}
													} else {
														if (arg0[seq].length() < arg1[seq].length()) {
															if (s.isAsc()) {
																return -1;
															} else {
																return 1;
															}

														} else if (arg0[seq].length() > arg1[seq].length()) {
															if (s.isAsc()) {
																return 1;
															} else {
																return -1;
															}

														} else {
															int res = arg0[seq].compareTo(arg1[seq]);
															if (res != 0) {

																if (s.isAsc()) {
																	return res;
																} else {
																	return -res;
																}
															}
														}
													}

												}
												return 0;
											}
										});

										BufferedWriter wcsv = new BufferedWriter(
												new FileWriter("data/" + order + "2" + fl + ".csv"));
										for (String towrite[] : tempstore) {

											// System.out.println(pc);
											// String[] towrite =
											// tempstore.get(pc);
											String str = "";
											for (String s : towrite) {
												str = str + s + "|";
											}
											str = str.substring(0, str.length() - 1);
											str += '\n';
											wcsv.write(str);
											nrow++;
											GRPN++;

										}
										// MAX_ROWS = PAGE;
										wcsv.close();
										fl++;
									}

									// System.out.println("nrow: " + nrow);
									MAX_ROWS = 99000;
									// System.exit(0);
									int numpages = (int) Math.ceil(nrow / PAGE);
									double k = MAX_ROWS / PAGE;
									// System.out.println(k + " " + numpages);
									int phases = (int) Math.ceil(Math.log(numpages / k) / Math.log(k - 1)) + 1;
									BufferedReader csv2 = null;
									BufferedReader csv = null;
									// System.out.println(" dgd " + phases);
									for (int p = 2; p <= phases; p++) {

										// System.out.println(p);

										double sortsize = k * Math.pow((k - 1), (p - 2)) * PAGE;
										double noinloops = (nrow / sortsize) / (k - 1);

										for (int j = 0; j < noinloops; j++) {
											BufferedWriter wcsv = new BufferedWriter(
													new FileWriter("data/" + order + (p + 1) + j + ".csv"));

											// System.out.println("in j :" + j);
											int ctr = 0;
											int ctr2 = 0;
											ArrayList<String[]> onetempstore = new ArrayList();
											ArrayList<String[]> twotempstore = new ArrayList();
											Iterator<String[]> oneit = onetempstore.iterator();
											oneit = null;
											Iterator<String[]> twoit = twotempstore.iterator();
											twoit = null;
											ArrayList<String[]> result = new ArrayList();
											int fflag = 0, check = 0, oneflag = 0, twoflag = 0;
											double off1 = 0, off2 = 0, limit1 = 0, limit2 = 0;
											String[] arg0 = null, arg1 = null;
											try {
												csv = new BufferedReader(
														new FileReader("data/" + order + p + (j * 2) + ".csv"));
												csv2 = new BufferedReader(
														new FileReader("data/" + order + p + (j * 2 + 1) + ".csv"));
											} catch (Exception e) {
											}
											while (true) {
												int nol = 0;

												if (fflag == 0) {
													off1 = 0;
													limit1 = sortsize;
													off2 = 0;
													limit2 = sortsize;

													if (j == Math.ceil(noinloops) - 1) {
														int row = (int) (nrow - j * sortsize * 2);
														int rows = (int) (row / sortsize);
														if (rows == 0) {
															limit2 = 0;
															limit1 = row;
														} else if (rows == 1) {
															limit1 = sortsize;
															limit2 = row % sortsize;
														} else {
															limit1 = sortsize;
															limit2 = sortsize;
														}
													}
												}
												// System.out.println("off:"+off1+"
												// l "+limit1+"off2: "+off2+" l2
												// "+limit2);

												if (limit2 == 0) {
													while ((read = csv.readLine()) != null) {
														// if (ctr >= off1) {
														// nol++;
														wcsv.write(read);
														wcsv.newLine();
														// }
														// ctr++;
													}
													wcsv.close();
													break;

												}

												// System.out.println("nnnoff:"+off1+"
												// l "+limit1+"off2: "+off2+" l2
												// "+limit2);

												if (oneit != null && !oneit.hasNext() && fflag == 1
														&& oneflag < sortsize / PAGE && arg0 == null) {

													onetempstore.clear();
													off1 += PAGE;
													oneflag++;
													oneit = null;
												}
												if (twoit != null && !twoit.hasNext() && fflag == 1
														&& twoflag < sortsize / PAGE && arg1 == null) {
													// System.out.println("twoit
													// nothing");
													twotempstore.clear();
													off2 += PAGE;
													twoflag++;
													twoit = null;
												}

												while (ctr >= off1 && ctr < off1 + PAGE && oneit == null
														&& oneflag < sortsize / PAGE
														&& (read = csv.readLine()) != null) {

													onetempstore.add(read.split("\\|"));
													ctr++;
												}
												while ((ctr2 >= off2 && ctr2 < off2 + PAGE && twoit == null
														&& twoflag < sortsize / PAGE
														&& (read2 = csv2.readLine()) != null)) {

													twotempstore.add(read2.split("\\|"));
													ctr2++;

												}
												// System.out.println("incoff:"+off1+"
												// l "+limit1+"off2: "+off2+" l2
												// "+limit2);
												// System.out.println("dats
												// "+oneflag+" t "+twoflag+" of
												// "+onetempstore.size()+" tw
												// "+twotempstore.size());
												if (oneit == null && oneflag < sortsize / PAGE && off1 < limit1) {
													oneit = onetempstore.iterator();
													arg0 = (String[]) oneit.next();
												}
												if (twoit == null && twoflag < sortsize / PAGE && off2 < limit2) {
													twoit = twotempstore.iterator();
													arg1 = (String[]) twoit.next();
												}
												// System.out.println("fffr");
												int floopy;

												int no = 0;
												while (arg0 != null && arg1 != null) {
													floopy = 0;
													if (result.size() < PAGE) {

														for (OrderByElement s : order) {
															int seq = schema.get(tname)
																	.get(s.getExpression().toString()).seq;

															if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

																int cmp;
																if (arg1[seq].contains("e")
																		|| arg0[seq].contains("e")) {
																	Double db0 = Double.parseDouble(arg0[seq]);
																	Double db1 = Double.parseDouble(arg1[seq]);
																	cmp = db0.compareTo(db1);
																	// return
																	// cmp;

																} else
																	cmp = comp(arg0[seq], arg1[seq]);

																if (cmp < 0) {
																	result.add(arg0);
																	no++;
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;
																	floopy = 1;
																	break;

																} else if (cmp > 0) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																}
															}

															else {

																if (arg0[seq].length() < arg1[seq].length()) {
																	no++;
																	result.add(arg0);
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;

																	floopy = 1;
																	break;

																} else if (arg0[seq].length() > arg1[seq].length()) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																} else {
																	int res = arg0[seq].compareTo(arg1[seq]);
																	if (res < 0) {
																		result.add(arg0);
																		no++;
																		if (oneit.hasNext())
																			arg0 = oneit.next();
																		else
																			arg0 = null;
																		floopy = 1;
																		break;

																	} else if (res > 0) {
																		result.add(arg1);
																		if (twoit.hasNext())
																			arg1 = twoit.next();
																		else
																			arg1 = null;
																		floopy = 1;
																		break;

																	}

																}

															}
														}
														if (floopy == 0) {
															no++;
															result.add(arg0);
															if (oneit.hasNext()) {
																arg0 = oneit.next();
															} else {
																arg0 = null;
															}

														}

													}
													if (result.size() == PAGE) {
														// System.out.println("PAGES:
														// " +
														// no);
														// System.out.println("printing");
														check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();
													}

												}

												fflag = 1;
												// System.out.println("one: " +
												// oneflag + " s " + sortsize /
												// PAGE);
												// System.out.println("off1 " +
												// off1
												// + " limit " + limit1);
												// System.out.println("off2 " +
												// off2
												// + " limit " + limit2);
												if (oneflag == sortsize / PAGE || off1 + PAGE >= limit1) {

													if (!result.isEmpty()) {
														// check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();

													}
													// check++;
													// System.out.println("here");
													if (arg1 != null) {
														check++;
														String str = "";
														for (String s : arg1) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
														while (twoit.hasNext()) {
															String[] resadd = (String[]) twoit.next();
															str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}
															str = str.substring(0, str.length() - 1);
															str += '\n';
															nol++;
															wcsv.write(str);
														}
														arg1 = null;
														// System.out.println("off2
														// " + off2
														// + " limit " +
														// limit2);
														if (off2 + PAGE >= limit2 && j == Math.ceil(noinloops) - 1)
															break;
													}
												}
												// System.out.println("two: " +
												// twoflag + " c " + check);
												if (twoflag == sortsize / PAGE || off2 + PAGE >= limit2) {

													if (!result.isEmpty()) {
														// check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();

													}
													// check++;
													if (arg0 != null) {
														check++;
														String str = "";
														for (String s : arg0) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
														while (oneit.hasNext()) {
															// System.out.print("!hr!");
															String[] resadd = (String[]) oneit.next();
															str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}
															str = str.substring(0, str.length() - 1);
															str += '\n';
															nol++;
															wcsv.write(str);
														}
														arg0 = null;
														// System.out.println("off1
														// " + off1
														// + " limit " +
														// limit1);
														if (off1 + PAGE >= limit1 && j == Math.ceil(noinloops) - 1)
															break;
													}
												}
												// System.out.println("CHECK!!!!!!!!!!!!!!!!!!!!!!!:
												// " + check);
												// System.out.println("LINES: "
												// +
												// nol);
												if (check == (k - 1) * (sortsize / PAGE)
														|| oneflag + twoflag == (k - 1) * (sortsize / PAGE))
													break;

											}
											wcsv.close();
											csv.close();
											csv2.close();

										}

									}
									backpk.clear();
									backpk.add("data/" + order + (phases + 1) + "0.csv");

								}

							} else if (group != null && ps.getJoins() == null) {

								if (alsort != 1) {

									String read = "";
									String read2 = "";
									Iterator<String> fit = backpk.iterator();
									int fl = 0;
									int nrow = 0;

									while (fit.hasNext()) {

										BufferedReader csv = new BufferedReader(new FileReader(fit.next()));
										ArrayList<String[]> tempstore = new ArrayList();

										while ((read = csv.readLine()) != null) {

											tempstore.add(read.split("\\|"));

										}
										csv.close();

										Collections.sort(tempstore, new Comparator<String[]>() {

											@Override
											public int compare(String[] arg0, String[] arg1) {
												for (Column s : group) {

													int seq = ((ColData) ((LinkedHashMap) schema.get(tname))
															.get(s.toString())).seq;

													if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

														int cmp;
														if (arg1[seq].contains("e") || arg0[seq].contains("e")) {
															Double db0 = Double.parseDouble(arg0[seq]);
															Double db1 = Double.parseDouble(arg1[seq]);
															cmp = db0.compareTo(db1);
															// return cmp;

														} else
															cmp = comp(arg0[seq], arg1[seq]);

														if (cmp != 0) {

															return cmp;

														}
													} else {
														if (arg0[seq].length() < arg1[seq].length()) {

															return -1;

														} else if (arg0[seq].length() > arg1[seq].length()) {

															return 1;

														} else {
															int res = arg0[seq].compareTo(arg1[seq]);
															if (res != 0) {

																return res;

															}
														}
													}

												}
												return 0;
											}
										});

										BufferedWriter wcsv = new BufferedWriter(
												new FileWriter("data/" + group + "2" + fl + ".csv"));
										for (String towrite[] : tempstore) {

											// System.out.println(pc);
											// String[] towrite =
											// tempstore.get(pc);
											String str = "";
											for (String s : towrite) {
												str = str + s + "|";
											}
											str = str.substring(0, str.length() - 1);
											str += '\n';
											wcsv.write(str);
											nrow++;
											GRPN++;

										}
										// MAX_ROWS = PAGE;
										wcsv.close();
										fl++;
									}

									// System.out.println("nrow: " + nrow);
									MAX_ROWS = 99000;
									// System.exit(0);
									int numpages = (int) Math.ceil(nrow / PAGE);
									double k = MAX_ROWS / PAGE;
									// System.out.println(k + " " + numpages);
									int phases = (int) Math.ceil(Math.log(numpages / k) / Math.log(k - 1)) + 1;
									BufferedReader csv2 = null;
									BufferedReader csv = null;
									// System.out.println(" dgd " + phases);
									for (int p = 2; p <= phases; p++) {

										// System.out.println(p);

										double sortsize = k * Math.pow((k - 1), (p - 2)) * PAGE;
										double noinloops = (nrow / sortsize) / (k - 1);

										for (int j = 0; j < noinloops; j++) {
											BufferedWriter wcsv = new BufferedWriter(
													new FileWriter("data/" + group + (p + 1) + j + ".csv"));

											// System.out.println("in j :" + j);
											int ctr = 0;
											int ctr2 = 0;
											ArrayList<String[]> onetempstore = new ArrayList();
											ArrayList<String[]> twotempstore = new ArrayList();
											Iterator<String[]> oneit = onetempstore.iterator();
											oneit = null;
											Iterator<String[]> twoit = twotempstore.iterator();
											twoit = null;
											ArrayList<String[]> result = new ArrayList();
											int fflag = 0, check = 0, oneflag = 0, twoflag = 0;
											double off1 = 0, off2 = 0, limit1 = 0, limit2 = 0;
											String[] arg0 = null, arg1 = null;
											try {
												csv = new BufferedReader(
														new FileReader("data/" + group + p + (j * 2) + ".csv"));
												csv2 = new BufferedReader(
														new FileReader("data/" + group + p + (j * 2 + 1) + ".csv"));
											} catch (Exception e) {
											}
											while (true) {
												int nol = 0;

												if (fflag == 0) {
													off1 = 0;
													limit1 = sortsize;
													off2 = 0;
													limit2 = sortsize;

													if (j == Math.ceil(noinloops) - 1) {
														int row = (int) (nrow - j * sortsize * 2);
														int rows = (int) (row / sortsize);
														if (rows == 0) {
															limit2 = 0;
															limit1 = row;
														} else if (rows == 1) {
															limit1 = sortsize;
															limit2 = row % sortsize;
														} else {
															limit1 = sortsize;
															limit2 = sortsize;
														}
													}
												}
												if (limit2 == 0) {
													while ((read = csv.readLine()) != null) {
														// if (ctr >= off1) {
														// nol++;
														wcsv.write(read);
														wcsv.newLine();
														// }
														// ctr++;
													}
													wcsv.close();
													break;

												}

												if (oneit != null && !oneit.hasNext() && fflag == 1
														&& oneflag < sortsize / PAGE && arg0 == null) {

													onetempstore.clear();
													off1 += PAGE;
													oneflag++;
													oneit = null;
												}
												if (twoit != null && !twoit.hasNext() && fflag == 1
														&& twoflag < sortsize / PAGE && arg1 == null) {
													// System.out.println("twoit
													// nothing");
													twotempstore.clear();
													off2 += PAGE;
													twoflag++;
													twoit = null;
												}

												while (ctr >= off1 && ctr < off1 + PAGE && oneit == null
														&& oneflag < sortsize / PAGE
														&& (read = csv.readLine()) != null) {

													onetempstore.add(read.split("\\|"));
													ctr++;
												}
												while ((ctr2 >= off2 && ctr2 < off2 + PAGE && twoit == null
														&& twoflag < sortsize / PAGE
														&& (read2 = csv2.readLine()) != null)) {

													twotempstore.add(read2.split("\\|"));
													ctr2++;

												}

												if (oneit == null && oneflag < sortsize / PAGE && off1 < limit1) {
													oneit = onetempstore.iterator();
													arg0 = (String[]) oneit.next();
												}
												if (twoit == null && twoflag < sortsize / PAGE && off2 < limit2) {
													twoit = twotempstore.iterator();
													arg1 = (String[]) twoit.next();
												}
												// System.out.println("fffr");
												int floopy;

												int no = 0;
												while (arg0 != null && arg1 != null) {
													floopy = 0;
													if (result.size() < PAGE) {

														for (Column s : group) {

															int seq = ((ColData) ((LinkedHashMap) schema.get(tname))
																	.get(s.toString())).seq;

															if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

																int cmp;
																if (arg1[seq].contains("e")
																		|| arg0[seq].contains("e")) {
																	Double db0 = Double.parseDouble(arg0[seq]);
																	Double db1 = Double.parseDouble(arg1[seq]);
																	cmp = db0.compareTo(db1);
																	// return
																	// cmp;

																} else
																	cmp = comp(arg0[seq], arg1[seq]);

																if (cmp < 0) {
																	result.add(arg0);
																	no++;
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;
																	floopy = 1;
																	break;

																} else if (cmp > 0) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																}
															}

															else {

																if (arg0[seq].length() < arg1[seq].length()) {
																	no++;
																	result.add(arg0);
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;

																	floopy = 1;
																	break;

																} else if (arg0[seq].length() > arg1[seq].length()) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																} else {
																	int res = arg0[seq].compareTo(arg1[seq]);
																	if (res < 0) {
																		result.add(arg0);
																		no++;
																		if (oneit.hasNext())
																			arg0 = oneit.next();
																		else
																			arg0 = null;
																		floopy = 1;
																		break;

																	} else if (res > 0) {
																		result.add(arg1);
																		if (twoit.hasNext())
																			arg1 = twoit.next();
																		else
																			arg1 = null;
																		floopy = 1;
																		break;

																	}

																}

															}
														}
														if (floopy == 0) {
															no++;
															result.add(arg0);
															if (oneit.hasNext()) {
																arg0 = oneit.next();
															} else {
																arg0 = null;
															}

														}

													}
													if (result.size() == PAGE) {
														// System.out.println("PAGES:
														// " +
														// no);
														// System.out.println("printing");
														check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();
													}

												}

												fflag = 1;

												if (oneflag == sortsize / PAGE || off1 + PAGE >= limit1) {

													if (!result.isEmpty()) {
														// check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();

													}
													// check++;
													// System.out.println("here");
													if (arg1 != null) {
														check++;
														String str = "";
														for (String s : arg1) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
														while (twoit.hasNext()) {
															String[] resadd = (String[]) twoit.next();
															str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}
															str = str.substring(0, str.length() - 1);
															str += '\n';
															nol++;
															wcsv.write(str);
														}
														arg1 = null;
														// System.out.println("off2
														// " + off2
														// + " limit " +
														// limit2);
														if (off2 + PAGE >= limit2 && j == Math.ceil(noinloops) - 1)
															break;
													}
												}
												// System.out.println("two: " +
												// twoflag + "
												// c " + check);
												if (twoflag == sortsize / PAGE || off2 + PAGE >= limit2) {

													if (!result.isEmpty()) {
														// check++;
														for (String[] resadd : result) {
															String str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}

															str = str.substring(0, str.length() - 1);

															str += '\n';
															nol++;
															wcsv.write(str);

														}
														result.clear();

													}
													// check++;
													if (arg0 != null) {
														check++;
														String str = "";
														for (String s : arg0) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
														while (oneit.hasNext()) {
															// System.out.print("!hr!");
															String[] resadd = (String[]) oneit.next();
															str = "";
															for (String s : resadd) {
																str = str + s + "|";
															}
															str = str.substring(0, str.length() - 1);
															str += '\n';
															nol++;
															wcsv.write(str);
														}
														arg0 = null;
														// System.out.println("off1
														// " + off1
														// + " limit " +
														// limit1);
														if (off1 + PAGE >= limit1 && j == Math.ceil(noinloops) - 1)
															break;
													}
												}

												if (check == (k - 1) * (sortsize / PAGE)
														|| oneflag + twoflag == (k - 1) * (sortsize / PAGE))
													break;

											}
											wcsv.close();
											csv.close();
											csv2.close();

										}

									}
									backpk.clear();
									backpk.add("data/" + group + (phases + 1) + "0.csv");

								}

							}
							if (ps.getJoins() == null) {
								Iterator<String> pkit = backpk.iterator();
								// System.out.println(backpk);
								PrimitiveValue result = null;
								int counter = 0;
								int mycount = 0;
								int grpflag = 0;

								int numAggs = selectitem.size();
								Double totalArr[] = new Double[numAggs];
								Arrays.fill(totalArr, 0.0);
								int minflag = 0, i = mincount;
								ArrayList prev = new ArrayList();
								int wg = 0;
								if (wexp != null) {
									wg = wexp.toString().split("AND").length;
								}
								LinkedHashMap<String, ColData> colmap = schema.get(tname);
								Set<String> asd = colmap.keySet();
								int gcounter = 0;
								while (pkit.hasNext()) {
									int ins = 0;

									// String[] insert = new
									// String[selectitem.size()];
									int nof = 0;
									BufferedReader csvp = new BufferedReader(new FileReader(pkit.next()));
									ArrayList<String[]> tempstore = new ArrayList();
									String readv = "";

									while ((readv = csvp.readLine()) != null) {
										gcounter++;

										linevals = readv.split("\\|");

										nof = 0;
										if (wexp != null) {
											result = eval.eval(wexp);
											if (result.toBool()) {

												nof = 1;
											}

										}
										if (group == null && flag == 1)
											rcflag = 1;
										if (nof == 1 || result == null) {
											norow = 1;
											if (flag == 0) {

												if (lt != null) {

													if (counter < lt.getRowCount()) {
														counter++;
														Iterator selit = selectitem.iterator();
														while (selit.hasNext()) {

															Object selectFrom = selit.next();

															System.out.print(eval.eval(
																	((SelectExpressionItem) selectFrom).getExpression())
																	+ "|");

														}
														System.out.println("\b");

													} else
														break;
												} else {

													Iterator selit = selectitem.iterator();
													// System.out.println("print
													// 1
													// limit
													// null");
													while (selit.hasNext()) {
														Object selectFrom = selit.next();
														System.out.print(eval.eval(
																((SelectExpressionItem) selectFrom).getExpression())
																+ "|");

													}
													System.out.println("\b");
												}
											} else {
												if (group == null) {
													Iterator<SelectItem> si = selectitem.iterator();
													Iterator comparei = compare1.iterator();
													int index = 0;
													counter++;
													while (si.hasNext()) {
														SelectExpressionItem selectFrom = (SelectExpressionItem) si
																.next();
														Expression e = selectFrom.getExpression();
														Function f = (Function) e;
														int nom = (int) comparei.next();
														if ((nom == 1)) {
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															totalArr[index] = totalArr[index] + res.toDouble();
															index++;
														} else if (nom == 4) {
															totalArr[index]++;
															index++;
														} else if (nom == 3) {

															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															if (res.toDouble() > totalArr[index])
																totalArr[index] = res.toDouble();
															index++;
														} else if (nom == 2) {
															if (minflag < i) {
																// System.out.println(""+minflag+i);
																totalArr[index] = Double.MAX_VALUE;
																minflag++;
															}
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															if (res.toDouble() < totalArr[index])
																totalArr[index] = res.toDouble();
															index++;
														} else if (nom == 5) {
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															totalArr[index] = totalArr[index] * (counter - 1)
																	+ res.toDouble();
															totalArr[index] = totalArr[index] / counter;
															index++;

														}
													}
												} else {
													ArrayList curr = new ArrayList();
													Iterator grp = group.iterator();
													while (grp.hasNext()) {
														Column colgrp = (Column) grp.next();
														int seq = ((ColData) ((LinkedHashMap) schema.get(tname))
																.get(colgrp.toString())).seq;
														curr.add(linevals[seq]);
														if (grpflag == 0) {
															prev.add(linevals[seq]);
														}

													}
													grpflag = 1;
													if (!curr.equals(prev)) {
														long ctr;

														if (lt != null) {
															ctr = lt.getRowCount();
														} else
															ctr = Long.MAX_VALUE;
														if (mycount == ctr)
															break;
														mycount++;

														Iterator previous = prev.iterator();
														ctr++;
														while (previous.hasNext()) {
															System.out.print(previous.next() + "|");
															// insert[ins++] =
															// (String)
															// previous.next();
														}

														for (int k = 0; k < numAggs; k++) {
															if (totalArr[k] != -1.0) {
																System.out.print(totalArr[k] + "|");
																// insert[ins++]
																// =
																// totalArr[k].toString();
															}

														}
														System.out.println("\b");
														Arrays.fill(totalArr, 0.0);
														minflag = 0;
														counter = 0;
														prev = (ArrayList) curr.clone();

													}

													Iterator<SelectItem> si = selectitem.iterator();
													Iterator comparei = compare1.iterator();
													int index = 0;
													counter++;

													while (si.hasNext()) {
														SelectItem selectFrom = si.next();
														int nom = (int) comparei.next();
														if (nom == 0) {
															totalArr[index] = -1.0;
															index++;
															continue;

														}
														Expression e = ((SelectExpressionItem) selectFrom)
																.getExpression();

														Function f = (Function) e;

														if ((nom == 1)) {
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															totalArr[index] = totalArr[index] + res.toDouble();
															index++;
														} else if (nom == 4) {
															totalArr[index]++;
															index++;
														} else if (nom == 3) {
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															if (res.toDouble() > totalArr[index])
																totalArr[index] = res.toDouble();
															index++;
														} else if (nom == 2) {
															if (minflag < i) {
																// System.out.println(""+minflag+i);
																totalArr[index] = Double.MAX_VALUE;
																minflag++;
															}
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															if (res.toDouble() < totalArr[index])
																totalArr[index] = res.toDouble();
															index++;
														} else if (nom == 5) {
															PrimitiveValue res = eval
																	.eval(f.getParameters().getExpressions().get(0));
															totalArr[index] = totalArr[index] * (counter - 1)
																	+ res.toDouble();
															totalArr[index] = totalArr[index] / counter;
															index++;

														}
													}
													prev = (ArrayList) curr.clone();

												}
												// only agg
												// agg groupby
												// flag=1 agg
											}
										}
										// System.out.println(GRPN+"
										// "+gcounter+"
										// "+norow);
										// System.out.println(NUMROWS+"
										// "+gcounter);
										if ((NUMROWS == gcounter && group != null && norow == 1)
												|| (gcounter == GRPN && group != null && norow == 1)) {
											// System.out.println("hereeee");

											long ctr;

											if (lt != null) {
												ctr = lt.getRowCount();
											} else
												ctr = Long.MAX_VALUE;
											if (mycount == ctr)
												break;
											mycount++;

											Iterator previous = prev.iterator();
											ctr++;

											ins = 0;
											while (previous.hasNext()) {
												// String insertp = (String)
												// previous.next();
												System.out.print(previous.next() + "|");
												// insert2[ins++] = (String)
												// insertp;
											}

											for (int k = 0; k < numAggs; k++) {
												if (totalArr[k] != -1.0) {
													System.out.print(totalArr[k] + "|");
													// insert2[ins++] =
													// totalArr[k].toString();
												}
											}
											System.out.println("\b");

										}

									}
								}

								if (flag == 1 && group == null && norow == 1) {
									int ins = 0;
									// String[] insert = new
									// String[selectitem.size()];
									// grpby flag
									for (int k = 0; k < numAggs; k++) {
										System.out.print(totalArr[k] + "|");
										// insert[ins++] =
										// totalArr[k].toString();

									}
									System.out.println("\b");

								}

								qflag = 1;
								System.gc();

							}
							// System.out.println(norow);
							if (resflag == 1 || norow == 0) {
								int i = 0;
								for (SelectItem s : ps.getSelectItems()) {
									if (rcflag == 1) {
										if (compare1.get(i) == 4) {
											System.out.print("0");
										}
										if ((compare1.size() > 1 || compare1.get(0) == 4))
											System.out.print("|");
									}
									i++;
								}
								// System.out.println(compare1.size()+
								// ""+compare1+"");
								if (rcflag == 1 && (compare1.size() > 1 || compare1.get(0) == 4))
									System.out.println("\b");

							}
						} else {

							Expression whr = ps.getWhere();
							ArrayList<String> pkstore = new ArrayList<String>();
							ArrayList<String> backpk = new ArrayList<String>();
							int whflag = 0;
							int empflag = 0;
							int whreval = 0;
							String obj = null;
							Boolean hjk = true;
							int lsdfl = 0;

							ArrayList<String> obj1 = new ArrayList<String>();
							ArrayList<BinaryExpression> jlist = new ArrayList();
							ArrayList<BinaryExpression> evlist = new ArrayList();
							Map<String, ArrayList<BinaryExpression>> whmap = new HashMap();
							Map<String, Integer> empmap = new HashMap();
							Map<String, ArrayList<String>> bkmap = new HashMap();
							// System.out.println(wexp + " dfg " + query);

							Expression lsd = null;
							Expression lsd2 = null;
							if (wexp != null && qflag == 0) {

								ArrayList<Expression> qwe = new ArrayList();
								if (whr instanceof AndExpression) {
									while (whr instanceof AndExpression) {
										qwe.add(((AndExpression) whr).getRightExpression());

										whr = ((AndExpression) whr).getLeftExpression();
									}
									qwe.add(whr);
								}

								else if (whr instanceof OrExpression) {
									OrExpression asd = new OrExpression();
									Expression e = (AndExpression) ((OrExpression) whr).getRightExpression();
									while (e instanceof AndExpression) {
										qwe.add(((AndExpression) e).getRightExpression());
										e = ((AndExpression) e).getLeftExpression();

									}
									asd.setRightExpression(e);
									e = (AndExpression) ((OrExpression) whr).getLeftExpression();
									asd.setLeftExpression(((AndExpression) e).getRightExpression());
									e = ((AndExpression) e).getLeftExpression();
									while (e instanceof AndExpression) {
										qwe.add(((AndExpression) e).getRightExpression());
										e = ((AndExpression) e).getLeftExpression();

									}
									qwe.add(e);
									evlist.add(asd);
									whmap.put("lineitem", evlist);
								} else {
									qwe.add(whr);
								}

								String min = null;
								String max = null;
								int remi = 0;
								int mini = 0, maxi = 0;
								for (Expression m : qwe) {

									BinaryExpression k = (BinaryExpression) m;

									// System.out.println(k);
									if (k.getLeftExpression() instanceof Column
											&& k.getRightExpression() instanceof Column
											&& !(((Column) k.getLeftExpression()).getTable().getName()
													.equals(((Column) k.getLeftExpression()).getTable().getName()))) {
										// jlist.add(k);
										continue;
									}

									if (k.getLeftExpression() instanceof Column) {
										// System.out.println(((Column)k.getLeftExpression()).getTable().getName());
										if (k.getLeftExpression().toString().equals("lineitem.receiptdate")
												|| k.getLeftExpression().toString().equals("orders.orderdate")) {
											if (k instanceof GreaterThanEquals) {
												min = k.getRightExpression().toString().substring(6,
														k.getRightExpression().toString().length() - 2);

												mini = remi;
											} else if (k instanceof GreaterThan) {
												min = LocalDate
														.parse(k.getRightExpression().toString().substring(6,
																k.getRightExpression().toString().length() - 2))
														.plusDays(1).toString();

												mini = remi;
											} else if (k instanceof MinorThan) {
												max = LocalDate
														.parse(k.getRightExpression().toString().substring(6,
																k.getRightExpression().toString().length() - 2))
														.minusDays(1).toString();

												maxi = remi;
											} else if (k instanceof MinorThanEquals) {
												max = k.getRightExpression().toString().substring(6,
														k.getRightExpression().toString().length() - 2);

												maxi = remi;
											}

										}

									} else {
										if (k.getRightExpression().toString().equals("lineitem.receiptdate")
												|| k.getRightExpression().toString().equals("orders.orderdate")) {
											if (k instanceof GreaterThanEquals) {
												min = k.getLeftExpression().toString().substring(6,
														k.getLeftExpression().toString().length() - 2);

												mini = remi;
											} else if (k instanceof GreaterThan) {
												min = LocalDate
														.parse(k.getLeftExpression().toString().substring(6,
																k.getLeftExpression().toString().length() - 2))
														.plusDays(1).toString();

												mini = remi;

											} else if (k instanceof MinorThan) {
												max = LocalDate
														.parse(k.getLeftExpression().toString().substring(6,
																k.getLeftExpression().toString().length() - 2))
														.minusDays(1).toString();

												maxi = remi;
											} else if (k instanceof MinorThanEquals) {
												max = k.getLeftExpression().toString().substring(6,
														k.getLeftExpression().toString().length() - 2);

												maxi = remi;
											}

										}

									}
									remi++;

								}
								if (max != null && min != null) {
									AndExpression ad = new AndExpression();
									ad.setLeftExpression(qwe.get(mini));
									ad.setRightExpression(qwe.get(maxi));
									lsd = ad;
								}

								String value;

								String col;
								String wheres[] = wexp.toString().split("AND");
								int pkflag = 0;
								for (Expression e : qwe) {
									evlist = new ArrayList();
									String s = e.toString();
									if (s.contains("lineitem.returnflag")) {
										lsd2 = e;
										lsdfl = 1;

									}

									BinaryExpression k = (BinaryExpression) e;

									if (k.getLeftExpression() instanceof Column
											&& k.getRightExpression() instanceof Column
											&& !(((Column) k.getLeftExpression()).getTable().getName()
													.equals(((Column) k.getRightExpression()).getTable().getName()))) {

										jlist.add(k);
										continue;
									}
									if (k.getLeftExpression() instanceof Column
											&& k.getRightExpression() instanceof Column) {

										tname = ((Column) k.getLeftExpression()).getTable().getName();
										if (whmap.containsKey(tname)) {
											evlist = whmap.get(tname);
											evlist.add(k);

										} else {
											evlist.add(k);
											whmap.put(tname, evlist);

										}

										// evlist.add(k);
										continue;
									}

									if (k.getLeftExpression() instanceof Column) {
										// System.out.println(((Column)
										// k.getLeftExpression()).getTable().getName());
										tname = ((Column) k.getLeftExpression()).getTable().getName();
									} else {
										tname = ((Column) k.getRightExpression()).getTable().getName();
									}

									all = allmap.get(tname);

									whflag = 0;
									pkstore.clear();
									Iterator<ArrayList<String>> ind = all.keySet().iterator();
									while (ind.hasNext()) {
										obj1 = ind.next();
										obj = obj1.get(0);
										ColData id = schema.get(tname).get(obj);
										// System.out.println(s+""+obj);
										if (s.contains(obj)) {
											// System.out.println(s+" o "+obj);
											whreval++;
											empflag = 1;
											empmap.put(tname, empflag);
											whflag = 1;
											TreeMap<ArrayList<String>, ArrayList<String>> in = all.get(obj1);
											Iterator<ArrayList<String>> idd = in.keySet().iterator();
											if (s.contains(">=")) {
												value = s.split(">=")[1];
												col = s.split(">=")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(1).toString(), value) <= 0) {
															pkstore.addAll(in.get(keyp));

														} else {
															pkstore.addAll(in.get(keyp));
															break;
														}
													}
												} else {
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if ((compstr(keyp.get(0).toString(), value) <= 0
																&& compstr(keyp.get(1).toString(), value) >= 0)
																|| (compstr(keyp.get(0).toString(), value) >= 0
																		&& compstr(keyp.get(1).toString(),
																				value) >= 0)) {
															pkstore.addAll(in.get(keyp));
															while (idd.hasNext()) {
																keyp = idd.next();
																pkstore.addAll(in.get(keyp));
															}
														}
													}
												}
											} else if (s.contains("<=")) {

												value = s.split("<=")[1];
												col = s.split("<=")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if ((compstr(keyp.get(0).toString(), value) <= 0
																&& compstr(keyp.get(1).toString(), value) >= 0)
																|| (compstr(keyp.get(0).toString(), value) >= 0
																		&& compstr(keyp.get(1).toString(),
																				value) >= 0)) {
															pkstore.addAll(in.get(keyp));
															while (idd.hasNext()) {
																keyp = idd.next();
																pkstore.addAll(in.get(keyp));
															}
														}
													}

												} else {

													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(1).toString(), value) <= 0) {
															pkstore.addAll(in.get(keyp));

														} else {
															pkstore.addAll(in.get(keyp));
															break;
														}
													}
												}
											} else if (s.contains("<>")) {
												// continue
												whflag = 0;
												empflag = 0;
												whreval--;

											} else if (s.contains("<")) {
												value = s.split("<")[1];
												col = s.split("<")[0].trim();

												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													// value =
													// String.valueOf((Integer.parseInt(value)
													// + 1));
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (!(compstr(keyp.get(0).toString(), value) < 0
																&& compstr(keyp.get(1).toString(), value) < 0)) {
															pkstore.addAll(in.get(keyp));
														}
													}
												} else {

													value = value.trim();

													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(0).toString(), value) < 0) {
															pkstore.addAll(in.get(keyp));
														} else {
															break;
														}

													}
												}
											} else if (s.contains(">")) {
												value = s.split(">")[1];
												col = s.split(">")[0].trim();
												if (!col.contains(obj)) {
													value = col;
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (compstr(keyp.get(0).toString(), value) < 0) {
															pkstore.addAll(in.get(keyp));
														} else {
															break;
														}

													}
												} else {
													value = value.trim();
													while (idd.hasNext()) {
														ArrayList<String> keyp = idd.next();
														if (!(compstr(keyp.get(0).toString(), value) < 0
																&& compstr(keyp.get(1).toString(), value) < 0)) {
															pkstore.addAll(in.get(keyp));
														}
													}

												}
											}

											else if (s.contains("=")) {
												value = s.split("=")[1];
												value = value.trim();
												if (value.contains(obj)) {
													value = s.split("=")[0].trim();
												}
												int f = 0;
												while (idd.hasNext()) {
													ArrayList<String> keyp = idd.next();
													if (compstr(keyp.get(0).toString(), value) <= 0
															&& compstr(keyp.get(1).toString(), value) >= 0) {
														f = 1;
														pkstore.addAll(in.get(keyp));
													} else if (f == 1)
														break;
												}

											}
											break;

										}

									}

									if (whflag == 0) {
										// tname = ((Column)
										// k.getLeftExpression()).getTable().getName();
										if (whmap.containsKey(tname)) {
											evlist = whmap.get(tname);
											evlist.add(k);

										} else {
											evlist.add(k);
											whmap.put(tname, evlist);

										}

										// evlist.add(k);
									}

									if (!bkmap.containsKey(tname) && whflag == 1) {
										if (!pkstore.isEmpty()) {
											backpk = (ArrayList<String>) pkstore.clone();
											bkmap.put(tname, backpk);

										} else {
											resflag = 1;
											break;
										}

									}

									else if (whflag == 1) {
										backpk = bkmap.get(tname);
										if (!pkstore.isEmpty()) {

											// System.out.println("sdf"+bkstore.size());
											backpk.retainAll(pkstore);
											if (backpk.size() == 0) {
												resflag = 1;
												break;
											}

											bkmap.put(tname, backpk);

										} else {
											resflag = 1;
											backpk.clear();
											break;
										}

									}

								}

							}
							int alsort = 0;
							int lflag = 0;
							int flg = 0;
							TreeMap<ArrayList<String>, String[]> data;
							Map<String, ArrayList<String[]>> pkmap = new HashMap();
							ArrayList<String[]> pksto = new ArrayList();
							if (ps.getJoins() == null) {

								// System.out.println(group+" "+backpk+" s
								// "+all.keySet());

								if (backpk.isEmpty() && !empmap.containsKey(tname)) {
									all = allmap.get(tname);
									if (order != null) {
										ArrayList<String> orderal = new ArrayList<String>();
										for (OrderByElement s : order) {
											orderal.add(s.toString());
										}
										if ((all.containsKey(orderal))) {
											alsort = 1;
											// System.out.println(all.get(orderal));
											for (ArrayList<String> ok : all.get(orderal).keySet()) {
												backpk.addAll(all.get(orderal).get(ok));
											}
											// ArrayList<String> tt =
											// (ArrayList<String>)
											// all.get(orderal).values();
											// backpk.addAll(tt);

										}

									}

								} else if (backpk.isEmpty()) {
									resflag = 1;
									// System.out.println("no values");
									break;
								}
							} else {
								backpk = new ArrayList();
								Iterator<String> itm = bkmap.keySet().iterator();
								BufferedWriter wcsvp = new BufferedWriter(new FileWriter("lineitemlsd.csv"));
								BufferedWriter wcsvp1 = new BufferedWriter(new FileWriter("ordersod.csv"));

								while (itm.hasNext()) {
									backpk = new ArrayList();
									String key = itm.next();
									if (key.equals("lineitem")) {
										tname = key;

										backpk.addAll(bkmap.get(key));
									} else {
										tname = key;
										flg = 1;
										backpk.addAll(bkmap.get(key));
									}

									Iterator<String> pkit = backpk.iterator();
									// System.out.println(backpk);
									PrimitiveValue result = null;

									LinkedHashMap<String, ColData> colmap = schema.get(tname);
									Set<String> asd = colmap.keySet();
									int gcounter = 0;

									if (whmap.containsKey("lineitem")) {
										ArrayList<BinaryExpression> llist = whmap.get("lineitem");
										Iterator<BinaryExpression> ls = llist.iterator();
										AndExpression ad = new AndExpression();
										int fl = 0;
										while (ls.hasNext()) {
											AndExpression as = new AndExpression();
											if (fl == 0) {

												as.setLeftExpression(lsd);
												fl = 1;
											} else {

												as.setLeftExpression(ad);
											}
											as.setRightExpression(ls.next());
											ad = as;
										}

										lsd = ad;

									}
									while (pkit.hasNext()) {
										int nof = 0;
										lflag = 1;
										BufferedReader csvp = new BufferedReader(new FileReader(pkit.next()));

										ArrayList<String[]> tempstore = new ArrayList();
										String readv = "";

										while ((readv = csvp.readLine()) != null) {

											linevals = readv.split("\\|");

											nof = 0;
											if (lsdfl == 1) {
												result = eval.eval(lsd2);
												if (result.toBool()) {
													wcsvp.write(readv);
													wcsvp.newLine();

												}

											} else if (flg == 1) {
												result = eval.eval(lsd);
												if (result.toBool()) {
													wcsvp1.write(readv);
													wcsvp1.newLine();

												}

											}

											else if (lsd != null) {
												result = eval.eval(lsd);
												if (result.toBool()) {
													wcsvp.write(readv);
													wcsvp.newLine();

												}

											}

										}

									}
									if (lsdfl == 1) {
										lsdfl = 0;
									}

								}
								wcsvp.close();
								wcsvp1.close();
							}
							// System.out.println(pkmap.size() + " p " +
							// pkstore.size() + " j " + jlist + " e " + whmap);

							if (whmap.size() > 0) {
							}

							Iterator<String> pku = whmap.keySet().iterator();
							while (pku.hasNext()) {
								String key = pku.next();
								tname = key;

								if (tname.equals("lineitem")) {
									continue;
								}
								if (!pkmap.containsKey(key)) {
									pksto = new ArrayList(datamap.get(key).values());
									pkmap.put(key, pksto);
								} else {
									pksto = pkmap.get(key);
								}
								Iterator<String[]> itm = pksto.iterator();
								ArrayList<String[]> tempu = new ArrayList();
								while (itm.hasNext()) {
									linevals = itm.next();
									PrimitiveValue result = null;
									int nof = 0;
									Iterator<BinaryExpression> wh = whmap.get(key).iterator();
									while (wh.hasNext()) {
										nof = 0;
										result = eval.eval(wh.next());
										if (result.toBool()) {
											nof = 1;
										} else {

											break;

										}

									}
									if (nof == 1) {
										tempu.add(linevals);

									}

								}
								// System.out.println("Refined: " +
								// tempu.size());
								if (tempu.size() == 0) {
									resflag = 1;
									break;
								}
								pkmap.put(key, tempu);

							}

							if (resflag == 1)
								break;

							if (ps.getJoins() != null) {
								if (!pkmap.containsKey(ps.getFromItem().toString())
										&& !ps.getFromItem().toString().equals("lineitem")) {

									pkmap.put(ps.getFromItem().toString(),
											new ArrayList(datamap.get(ps.getFromItem().toString()).values()));
								}
								ArrayList<HashMap<String, String[]>> join = new ArrayList();
								ArrayList<HashMap<String, String[]>> injoin = new ArrayList();

								Iterator<Join> jit = ps.getJoins().iterator();
								while (jit.hasNext()) {
									String key = jit.next().toString();
									if (!pkmap.containsKey(key) && !key.equals("lineitem") && !key.equals("orders")) {
										pkmap.put(key, new ArrayList(datamap.get(key).values()));
									}
								}
								pksto = new ArrayList();
								bkmap = new HashMap();
								backpk = new ArrayList();
								data = new TreeMap();

								/*
								 * FileOutputStream fileOut = new
								 * FileOutputStream("data/datamap.ser");
								 * ObjectOutputStream out = new
								 * ObjectOutputStream(fileOut);
								 * out.writeObject(datamap); out.close();
								 * 
								 * datamap=new HashMap();
								 */

								Collections.reverse(jlist);

								ArrayList<BinaryExpression> jl = new ArrayList();
								if (jlist.size() == 6) {
									jl.add(jlist.get(5));
									jl.add(jlist.get(3));
									jl.add(jlist.get(0));
									jl.add(jlist.get(1));
									jl.add(jlist.get(2));
									jl.add(jlist.get(4));

									jlist = new ArrayList(jl);

								}

								Iterator<BinaryExpression> jadu = jlist.iterator();
								int jflag = 0;
								while (jadu.hasNext()) {
									HashMap<String, ArrayList<HashMap<String, String[]>>> joinmap = new HashMap();
									BinaryExpression k = jadu.next();

									if (jflag == 0) {
										if (jlist.size() == 1) {
											BufferedReader csd = new BufferedReader(new FileReader("lineitemlsd.csv"));
											String read = "";

											int sq = schema.get(tname).get("lineitem.orderkey").seq;
											HashMap<String, String[]> sdf = new HashMap();
											while ((read = csd.readLine()) != null) {
												String val[] = read.split("\\|");
												ArrayList<HashMap<String, String[]>> jf = new ArrayList();
												if (joinmap.containsKey(val[sq])) {
													sdf = new HashMap();
													jf = joinmap.get(val[sq]);
													sdf.put("lineitem", val);
													jf.add(sdf);

												} else {
													sdf = new HashMap();
													sdf.put("lineitem", val);
													jf.add(sdf);
													joinmap.put(val[sq], jf);
												}

											}
											sq = schema.get("orders").get("orders.orderkey").seq;

											csd = new BufferedReader(new FileReader("data/orders00.csv"));
											while ((read = csd.readLine()) != null) {
												String val[] = read.split("\\|");
												if (joinmap.containsKey(val[sq])) {
													ArrayList<HashMap<String, String[]>> bk = joinmap.get(val[sq]);
													Iterator<HashMap<String, String[]>> bm = bk.iterator();
													while (bm.hasNext()) {
														HashMap<String, String[]> put = bm.next();
														HashMap<String, String[]> putnew = new HashMap<String, String[]>();
														Iterator<String> hashit = put.keySet().iterator();
														while (hashit.hasNext()) {
															String pva = hashit.next();
															putnew.put(pva, put.get(pva));
														}
														putnew.put("orders", val);
														join.add(putnew);

													}

												}

											}

											continue;

										}

										tname = ((Column) k.getLeftExpression()).getTable().getName();
										String lname = tname;

										String coln = k.getLeftExpression().toString();
										pksto = pkmap.get(tname);
										int sq = schema.get(tname).get(coln).seq;
										Iterator<String[]> pki = pksto.iterator();
										ArrayList<HashMap<String, String[]>> jf = new ArrayList();
										HashMap<String, String[]> sdf = new HashMap();
										while (pki.hasNext()) {
											String[] val = pki.next();
											jf = new ArrayList();
											if (joinmap.containsKey(val[sq])) {
												sdf = new HashMap();
												jf = joinmap.get(val[sq]);
												sdf.put(lname, val);
												jf.add(sdf);

											} else {
												sdf = new HashMap();
												sdf.put(lname, val);
												jf.add(sdf);
												joinmap.put(val[sq], jf);
											}
										}

										tname = ((Column) k.getRightExpression()).getTable().getName();
										if (tname.equals("orders")) {

											coln = k.getRightExpression().toString();
											sq = schema.get(tname).get(coln).seq;

											BufferedReader csd = new BufferedReader(new FileReader("ordersod.csv"));

											String read = "";
											while ((read = csd.readLine()) != null) {
												String val[] = read.split("\\|");
												if (joinmap.containsKey(val[sq])) {
													ArrayList<HashMap<String, String[]>> bk = joinmap.get(val[sq]);
													Iterator<HashMap<String, String[]>> bm = bk.iterator();
													while (bm.hasNext()) {
														HashMap<String, String[]> put = bm.next();
														HashMap<String, String[]> putnew = new HashMap<String, String[]>();
														Iterator<String> hashit = put.keySet().iterator();
														while (hashit.hasNext()) {
															String pva = hashit.next();
															putnew.put(pva, put.get(pva));
														}
														putnew.put(tname, val);
														join.add(putnew);

													}

												}

											}

										}

										jflag = 1;

									} else {
										if (join.size() > 0) {

											HashMap bsd = join.get(0);
											tname = ((Column) k.getLeftExpression()).getTable().getName();

											String rname = ((Column) k.getRightExpression()).getTable().getName();
											String lname = tname;
											String coln = k.getLeftExpression().toString();
											int sq = schema.get(lname).get(coln).seq;
											if (lname.equals("lineitem")) {
												tname = ((Column) k.getRightExpression()).getTable().getName();
												lname = tname;
												rname = ((Column) k.getLeftExpression()).getTable().getName();
												coln = k.getRightExpression().toString();
												sq = schema.get(lname).get(coln).seq;
											}

											if (bsd.keySet().contains(lname) && bsd.keySet().contains(rname)) {

												String rcoln = k.getRightExpression().toString();
												int rsq = schema.get(rname).get(rcoln).seq;

												Iterator<HashMap<String, String[]>> joinit = join.iterator();
												while (joinit.hasNext()) {
													HashMap<String, String[]> jabs = joinit.next();
													String lrow = jabs.get(lname)[sq].trim();
													String rrow = jabs.get(rname)[rsq].trim();
													if (lrow.equals(rrow)) {
														injoin.add(jabs);
													}
												}
											} else {
												if (bsd.keySet().contains(lname)) {
													Iterator<HashMap<String, String[]>> jm = join.iterator();
													while (jm.hasNext()) {
														HashMap<String, String[]> hm = jm.next();
														String[] arr = hm.get(lname);
														ArrayList<HashMap<String, String[]>> jf = new ArrayList();
														if (joinmap.containsKey(arr[sq])) {
															jf = joinmap.get(arr[sq]);
															jf.add(hm);

														} else {
															jf.add(hm);
															joinmap.put(arr[sq], jf);

														}

													}

												} else {

													pksto = pkmap.get(lname);

													Iterator<String[]> pki = pksto.iterator();
													while (pki.hasNext()) {
														String[] val = pki.next();
														ArrayList<HashMap<String, String[]>> jf = new ArrayList();
														if (joinmap.containsKey(val[sq])) {
															HashMap<String, String[]> sdf = new HashMap();

															jf = joinmap.get(val[sq]);
															sdf.put(lname, val);
															jf.add(sdf);

														} else {
															HashMap<String, String[]> sdf = new HashMap();
															sdf.put(lname, val);
															jf.add(sdf);
															joinmap.put(val[sq], jf);
														}
													}

												}

												if (rname.equals("lineitem")) {
													tname = "lineitem";
													coln = k.getLeftExpression().toString();
													sq = schema.get(tname).get(coln).seq;
													if (bsd.keySet().contains(tname)) {
														Iterator<HashMap<String, String[]>> jm = join.iterator();
														while (jm.hasNext()) {
															HashMap<String, String[]> hm = jm.next();
															String[] arr = hm.get(tname);
															ArrayList<HashMap<String, String[]>> jf = new ArrayList();
															if (joinmap.containsKey(arr[sq])) {
																Iterator<HashMap<String, String[]>> jk = joinmap
																		.get(arr[sq]).iterator();

																while (jk.hasNext()) {
																	HashMap<String, String[]> sdf = jk.next();

																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = hm.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, hm.get(pva));
																	}
																	putnew.put(lname, sdf.get(lname));
																	injoin.add(putnew);

																}

															}
														}

													} else {
														BufferedReader csd;
														if (lflag == 1) {
															csd = new BufferedReader(new FileReader("lineitemlsd.csv"));
														} else {
															csd = new BufferedReader(
																	new FileReader("data/LINEITEM.csv"));
														}
														String read = "";
														while ((read = csd.readLine()) != null) {
															String val[] = read.split("\\|");
															if (joinmap.containsKey(val[sq])) {
																ArrayList<HashMap<String, String[]>> bk = joinmap
																		.get(val[sq]);
																Iterator<HashMap<String, String[]>> bm = bk.iterator();
																while (bm.hasNext()) {
																	HashMap<String, String[]> put = bm.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = put.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, put.get(pva));
																	}
																	putnew.put(tname, val);
																	injoin.add(putnew);

																}

															}

														}

													}

												} else {
													tname = ((Column) k.getRightExpression()).getTable().getName();

													coln = k.getRightExpression().toString();
													sq = schema.get(tname).get(coln).seq;
													if (bsd.keySet().contains(tname)) {
														Iterator<HashMap<String, String[]>> jm = join.iterator();
														while (jm.hasNext()) {
															HashMap<String, String[]> hm = jm.next();
															String[] arr = hm.get(tname);
															ArrayList<HashMap<String, String[]>> jf = new ArrayList();
															if (joinmap.containsKey(arr[sq])) {
																Iterator<HashMap<String, String[]>> jk = joinmap
																		.get(arr[sq]).iterator();

																while (jk.hasNext()) {
																	HashMap<String, String[]> sdf = jk.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = hm.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, hm.get(pva));
																	}
																	putnew.put(lname, sdf.get(lname));
																	injoin.add(putnew);
																}

															}
														}

													} else {

														pksto = pkmap.get(tname);

														Iterator<String[]> pki = pksto.iterator();
														while (pki.hasNext()) {
															String[] val = pki.next();
															if (joinmap.containsKey(val[sq])) {
																ArrayList<HashMap<String, String[]>> bk = joinmap
																		.get(val[sq]);
																Iterator<HashMap<String, String[]>> bm = bk.iterator();
																while (bm.hasNext()) {
																	HashMap<String, String[]> put = bm.next();
																	HashMap<String, String[]> putnew = new HashMap<String, String[]>();
																	Iterator<String> hashit = put.keySet().iterator();
																	while (hashit.hasNext()) {
																		String pva = hashit.next();
																		putnew.put(pva, put.get(pva));
																	}
																	putnew.put(tname, val);
																	injoin.add(putnew);

																}

															}

														}

													}

												}
											}
										}
										join = new ArrayList();
										join = (ArrayList<HashMap<String, String[]>>) injoin.clone();
										injoin = new ArrayList();
									}

								}

								pksto = new ArrayList<String[]>();
								ArrayList<Integer> grpsq = new ArrayList<>();
								ArrayList<Integer> asq = new ArrayList<>();
								HashMap<ArrayList<String>, ArrayList<Double>> gdata = new HashMap();
								int m = 0;
								Iterator<HashMap<String, String[]>> getrow = join.iterator();
								int mflag = 0;

								while (getrow.hasNext()) {
									String[] gm = new String[selectitem.size()];
									rowmapo = getrow.next();
									Iterator<SelectItem> lis = selectitem.iterator();
									m = 0;
									while (lis.hasNext()) {

										SelectItem gplist = lis.next();
										Expression e = ((SelectExpressionItem) gplist).getExpression();

										if (e instanceof Function) {
											if (mflag == 0) {
												asq.add(m);
											}
											Function res = (Function) e;
											if (res.getName().equals("count")) {
												gm[m++] = "1";

											} else {

												Expression fi = res.getParameters().getExpressions().get(0);
												// System.out.println(fi);

												checkf = 1;
												gm[m++] = eval.eval(fi).toString();
												checkf = 0;

											}

										} else {
											if (mflag == 0) {
												grpsq.add(m);
											}
											String tn = ((Column) (e)).getTable().getName();
											String[] km = rowmapo.get(tn);
											int sq = schema.get(tn).get(gplist.toString()).seq;
											gm[m++] = km[sq];

										}

									}
									mflag = 1;
									ArrayList<String> keys = new ArrayList();
									Iterator<Integer> gsq = grpsq.iterator();
									while (gsq.hasNext()) {
										keys.add(gm[gsq.next()]);
									}
									if (gdata.containsKey(keys)) {

										ArrayList<Double> sc = gdata.get(keys);
										Iterator<Integer> asqi = asq.iterator();
										int ind = 0;
										while (asqi.hasNext()) {
											int sq = asqi.next();
											int tp = compare1.get(sq);
											Double dval = sc.get(ind);
											if (tp == 1) {
												dval += Double.valueOf(gm[sq]);
											} else if (tp == 2) {
												if (Double.valueOf(gm[sq]) < dval) {
													dval = Double.valueOf(gm[sq]);
												}

											} else if (tp == 3) {
												if (Double.valueOf(gm[sq]) > dval) {
													dval = Double.valueOf(gm[sq]);
												}

											} else if (tp == 4) {
												dval++;

											} else if (tp == 5) {
												// avg
											}

											sc.set(ind, dval);

											ind++;
										}
										gdata.put(keys, sc);

									} else {
										ArrayList<Double> sc = new ArrayList();
										Iterator<Integer> asqi = asq.iterator();
										while (asqi.hasNext()) {
											sc.add(Double.valueOf(gm[asqi.next()]));
										}
										gdata.put(keys, sc);
									}

								}

								for (ArrayList<String> fugi : gdata.keySet()) {
									String[] arrr = new String[selectitem.size()];
									Iterator<Integer> gsq = grpsq.iterator();
									int find = 0;
									while (gsq.hasNext()) {
										arrr[gsq.next()] = fugi.get(find);
										find++;
									}
									gsq = asq.iterator();
									find = 0;
									ArrayList<Double> qwa = gdata.get(fugi);
									while (gsq.hasNext()) {

										arrr[gsq.next()] = qwa.get(find).toString();
										find++;
									}

									pksto.add(arrr);

								}
								if (order != null) {
									List<Integer> osq = new ArrayList();
									for (OrderByElement s : order) {
										int os = 0;
										Iterator<SelectItem> lis = selectitem.iterator();

										while (lis.hasNext()) {
											SelectItem we = lis.next();
											String of = ((SelectExpressionItem) we).getAlias();

											if ((of != null && of.equals(s.getExpression().toString()))
													|| we.toString().equals(s.getExpression().toString())) {
												osq.add(os);
												break;
											}
											os++;

										}
									}

									Collections.sort(pksto, new Comparator<String[]>() {

										@Override
										public int compare(String[] arg0, String[] arg1) {
											int a = 0;
											for (OrderByElement s : order) {

												int seq = osq.get(a);
												a++;
												int cmp = 0;
												if (arg1[seq].contains(".") || arg0[seq].contains(".")) {

													if (arg1[seq].contains("e") || arg0[seq].contains("e")) {
														Double db0 = Double.parseDouble(arg0[seq]);
														Double db1 = Double.parseDouble(arg1[seq]);
														cmp = db0.compareTo(db1);

													} else
														cmp = comp(arg0[seq], arg1[seq]);

													if (cmp != 0) {
														if (s.isAsc()) {
															return cmp;
														} else {
															return -cmp;
														}
													}
												} else {
													if (arg0[seq].length() < arg1[seq].length()) {
														if (s.isAsc()) {
															return -1;
														} else {
															return 1;
														}

													} else if (arg0[seq].length() > arg1[seq].length()) {
														if (s.isAsc()) {
															return 1;
														} else {
															return -1;
														}

													} else {
														int res = arg0[seq].compareTo(arg1[seq]);
														if (res != 0) {

															if (s.isAsc()) {
																return res;
															} else {
																return -res;
															}
														}
													}
												}

											}
											return 0;
										}
									});

								}

								if (ps.getLimit() != null) {
									pksto = new ArrayList(pksto.subList(0, (int) ps.getLimit().getRowCount()));
								}
								for (String pr[] : pksto) {

									for (String s : pr) {

										System.out.print(s + "|");
									}
									System.out.println("\b");
								}

							}

						}
					}
				} else {

					if (tpflag == 0) {
						CreateTable cre = (CreateTable) query;
						String tablename = cre.getTable().toString();

						List cols = cre.getColumnDefinitions();
						Iterator i = cols.iterator();
						// System.out.println("sdfsd");
						LinkedHashMap<String, ColData> colmap = new LinkedHashMap<String, ColData>();
						int seqcounter = -1;
						while (i.hasNext()) {
							seqcounter++;
							Object col = i.next();
							String arr[] = col.toString().split(" ");
							ArrayList ab = new ArrayList();
							ColData colstat = new ColData(seqcounter, arr[1]);
							colmap.put(tablename + '.' + arr[0], colstat);
						}
						schema.put(tablename, colmap);

						indexes = new HashMap();

						name = "";
						int pflag = 0;
						int iflag = 0;
						List<Index> index = cre.getIndexes();
						if (index != null) {

							Iterator ind = index.iterator();
							while (ind.hasNext()) {
								// i_seq = new ArrayList();
								Index in = (Index) ind.next();
								if (in.getType().toUpperCase().equals("PRIMARY KEY")) {
									prim = in.getColumnsNames();
									Iterator pk = prim.iterator();
									while (pk.hasNext()) {
										ColData cd = (ColData) colmap.get(tablename + '.' + pk.next());
										pk_seq.add(cd.seq);
									}
									pflag = 1;
								} else {
									l_ind = in.getColumnsNames();
									ArrayList abc1 = new ArrayList();
									name = in.getName();
									Iterator iseq = l_ind.iterator();
									while (iseq.hasNext()) {
										Object ob = iseq.next();
										ColData cd = (ColData) colmap.get(tablename + '.' + ob);
										abc1.add(tablename + "." + ob);
										i_seq.add(cd.seq);
									}
									IndexData id = new IndexData(i_seq, abc1);
									indexes.put(name, id);
									iflag = 1;

								}
							}
							if (tablename.equals("lineitem")) {

								ArrayList<Integer> too = new ArrayList();
								ArrayList<String> tono = new ArrayList();
								too.add(10);
								tono.add("lineitem.shipdate");
								IndexData idm = new IndexData(too, tono);
								indexes.put("index_sp", idm);

								ArrayList<Integer> to = new ArrayList();
								ArrayList<String> ton = new ArrayList();
								to.add(8);
								to.add(9);
								ton.add("lineitem.returnflag");
								ton.add("lineitem.linestatus");
								idm = new IndexData(to, ton);
								indexes.put("index_lin", idm);

								too = new ArrayList();
								tono = new ArrayList();
								too.add(12);
								tono.add("lineitem.receiptdate");
								idm = new IndexData(too, tono);
								indexes.put("index_rec", idm);
								// System.out.println(indexes);
							} /*
								 * else if (tablename.equals("orders")) {
								 * 
								 * ArrayList<Integer> too = new ArrayList();
								 * ArrayList<String> tono = new ArrayList();
								 * too.add(4); tono.add("orders.orderdate");
								 * IndexData idm = new IndexData(too, tono);
								 * indexes.put("index_od", idm);
								 * 
								 * } else if (tablename.equals("customer")) {
								 * ArrayList<Integer> too = new ArrayList();
								 * ArrayList<String> tono = new ArrayList();
								 * too.add(3); tono.add("customer.nationkey");
								 * IndexData idm = new IndexData(too, tono);
								 * indexes.put("index_nk", idm);
								 * 
								 * }
								 */ else if (tablename.equals("nation") || tablename.equals("region")
									|| tablename.equals("supplier") || tablename.equals("customer")
									|| tablename.equals("orders")) {
								// assuming no small tables for on-disk

								TreeMap<ArrayList<String>, String[]> data = new TreeMap<ArrayList<String>, String[]>(
										new Comparator<ArrayList<String>>() {
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
										});

								String read = "";

								BufferedReader csv = new BufferedReader(
										new FileReader("data/" + tablename.toUpperCase() + ".csv"));
								// int check = 0;
								String reads[];
								Iterator<Integer> getpk;
								String patternString = "\\|";
								Pattern pattern = Pattern.compile(patternString);
								while ((read = csv.readLine()) != null) {
									reads = pattern.split(read);
									if (pflag == 1) {
										ArrayList pdata = new ArrayList();

										getpk = pk_seq.iterator();
										while (getpk.hasNext()) {
											pdata.add(reads[getpk.next()]);
										}
										data.put(pdata, reads);

									}
									// check = 1;
								}
								csv.close();
								System.gc();

								datamap.put(tablename, data);

							}

							Iterator<String> indexs = indexes.keySet().iterator();
							// System.out.println(indexes.size());
							while (indexs.hasNext()) {
								String dfg = indexs.next();
								// System.out.println(dfg);
								IndexData db = indexes.get(dfg);
								ArrayList<Integer> seqs = db.seq;
								List<String> colm = db.col;

								// System.out.println(colm+" sdsdf "+seqs);
								int rc = 0;
								int pc = 0;
								String read = "";
								String read2 = "";
								// System.out.println(tablename);
								BufferedReader csv = new BufferedReader(
										new FileReader("data/" + tablename.toUpperCase() + ".csv"));

								int fl = 0;
								int nrow = 0;
								while ((read = csv.readLine()) != null) {
									// System.out.println(read);
									ArrayList<String[]> tempstore = new ArrayList();
									while (rc < MAX_ROWS && read != null) {
										rc++;
										tempstore.add(read.split("\\|"));
										if (rc != MAX_ROWS)
											read = csv.readLine();

									}
									MAX_ROWS = rc;
									// System.out.println(tempstore.get(0)[0]);
									Collections.sort(tempstore, new Comparator<String[]>() {

										@Override
										public int compare(String[] arg0, String[] arg1) {
											for (int s : seqs) {
												if (arg1[s].contains(".") || arg0[s].contains(".")) {

													int cmp;
													if (arg1[s].contains("e") || arg0[s].contains("e")) {
														Double db0 = Double.parseDouble(arg0[s]);
														Double db1 = Double.parseDouble(arg1[s]);
														cmp = db0.compareTo(db1);
														// return cmp;

													} else
														cmp = comp(arg0[s], arg1[s]);

													if (cmp != 0) {

														return cmp;
													}
												} else {
													if (arg0[s].length() < arg1[s].length()) {

														return -1;

													} else if (arg0[s].length() > arg1[s].length()) {

														return 1;

													} else {
														int res = arg0[s].compareTo(arg1[s]);
														if (res != 0) {

															return res;

														}
													}
												}

											}
											return 0;
										}
									});
									rc = 0;
									// sort tempstore here
									pc = 0;
									BufferedWriter wcsv = new BufferedWriter(
											new FileWriter("data/index2" + fl + ".csv"));
									while (pc < MAX_ROWS) {

										// System.out.println(pc);
										String[] towrite = tempstore.get(pc);
										String str = "";
										for (String s : towrite) {
											str = str + s + "|";
										}
										str = str.substring(0, str.length() - 1);
										str += '\n';
										wcsv.write(str);
										nrow++;
										pc++;
										// remove from tempstore
									}
									// MAX_ROWS = PAGE;
									wcsv.close();
									fl++;
								}

								csv.close();
								// System.out.println("nrow: " + nrow);
								MAX_ROWS = 99000;
								NUMROWS = nrow;
								// System.exit(0);
								int numpages = (int) Math.ceil(nrow / PAGE);
								double k = MAX_ROWS / PAGE;
								// System.out.println(k + " " + numpages);
								int phases = (int) Math.ceil(Math.log(numpages / k) / Math.log(k - 1)) + 1;
								BufferedReader csv2 = null;
								System.out.println(" dgd " + phases);
								for (int p = 2; p <= phases; p++) {

									System.out.println(p);
									double sortsize = k * Math.pow((k - 1), (p - 2)) * PAGE;
									double noinloops = (nrow / sortsize) / (k - 1);

									for (int j = 0; j < noinloops; j++) {
										BufferedWriter wcsv = new BufferedWriter(
												new FileWriter("data/index" + (p + 1) + j + ".csv"));

										// System.out.println("in j :" + j);
										int ctr = 0;
										int ctr2 = 0;
										ArrayList<String[]> onetempstore = new ArrayList();
										ArrayList<String[]> twotempstore = new ArrayList();
										Iterator<String[]> oneit = onetempstore.iterator();
										oneit = null;
										Iterator<String[]> twoit = twotempstore.iterator();
										twoit = null;
										ArrayList<String[]> result = new ArrayList();
										int flag = 0, check = 0, oneflag = 0, twoflag = 0;
										double off1 = 0, off2 = 0, limit1 = 0, limit2 = 0;
										String[] arg0 = null, arg1 = null;
										try {
											csv = new BufferedReader(
													new FileReader("data/index" + p + (j * 2) + ".csv"));
											csv2 = new BufferedReader(
													new FileReader("data/index" + p + (j * 2 + 1) + ".csv"));
										} catch (Exception e) {
										}
										while (true) {
											int nol = 0;

											if (flag == 0) {
												off1 = 0;
												limit1 = sortsize;
												off2 = 0;
												limit2 = sortsize;

												if (j == Math.ceil(noinloops) - 1) {
													int row = (int) (nrow - j * sortsize * 2);
													int rows = (int) (row / sortsize);
													if (rows == 0) {
														limit2 = 0;
														limit1 = row;
													} else if (rows == 1) {
														limit1 = sortsize;
														limit2 = row % sortsize;
													} else {
														limit1 = sortsize;
														limit2 = sortsize;
													}
												}
											}
											if (limit2 == 0) {
												while ((read = csv.readLine()) != null) {
													// if (ctr >= off1) {
													// nol++;
													wcsv.write(read);
													wcsv.newLine();
													// }
													// ctr++;
												}
												wcsv.close();
												break;

											}

											if (oneit != null && !oneit.hasNext() && flag == 1
													&& oneflag < sortsize / PAGE && arg0 == null) {
												// System.out.println("inside 1
												// " +
												// oneflag);
												onetempstore.clear();
												off1 += PAGE;
												oneflag++;
												oneit = null;
											}
											if (twoit != null && !twoit.hasNext() && flag == 1
													&& twoflag < sortsize / PAGE && arg1 == null) {
												// System.out.println("twoit
												// nothing");
												twotempstore.clear();
												off2 += PAGE;
												twoflag++;
												twoit = null;
											}
											// System.out.println("ctr1: " + ctr
											// +
											// "ctr2: " + ctr2 + "oneflag: " +
											// oneflag
											// + "twoflag: " + twoflag);
											while (ctr >= off1 && ctr < off1 + PAGE && oneit == null
													&& oneflag < sortsize / PAGE && (read = csv.readLine()) != null) {

												onetempstore.add(read.split("\\|"));
												ctr++;
											}
											while ((ctr2 >= off2 && ctr2 < off2 + PAGE && twoit == null
													&& twoflag < sortsize / PAGE
													&& (read2 = csv2.readLine()) != null)) {

												twotempstore.add(read2.split("\\|"));
												ctr2++;

											}

											// System.out.println("dats size" +
											// onetempstore.size() + " ts " +
											// twotempstore.size()
											// + " o1 " + off1 + " o2 " + off2+"
											// l1
											// "+limit1+" l2 "+limit2);

											if (oneit == null && oneflag < sortsize / PAGE && off1 < limit1) {
												oneit = onetempstore.iterator();
												arg0 = (String[]) oneit.next();
											}
											if (twoit == null && twoflag < sortsize / PAGE && off2 < limit2) {
												twoit = twotempstore.iterator();
												arg1 = (String[]) twoit.next();
											}
											// System.out.println("fffr");
											int floopy;

											int no = 0;
											while (arg0 != null && arg1 != null) {
												floopy = 0;
												if (result.size() < PAGE) {
													for (int s : seqs) {

														if (arg1[s].contains(".") || arg0[s].contains(".")) {

															int cmp;
															if (arg1[s].contains("e") || arg0[s].contains("e")) {
																Double db0 = Double.parseDouble(arg0[s]);
																Double db1 = Double.parseDouble(arg1[s]);
																cmp = db0.compareTo(db1);
																// return cmp;

															} else
																cmp = comp(arg0[s], arg1[s]);

															if (cmp < 0) {
																result.add(arg0);
																no++;
																if (oneit.hasNext())
																	arg0 = oneit.next();
																else
																	arg0 = null;
																floopy = 1;
																break;

															} else if (cmp > 0) {
																result.add(arg1);
																if (twoit.hasNext())
																	arg1 = twoit.next();
																else
																	arg1 = null;
																floopy = 1;
																break;

															}
														}

														else {

															// System.out.println("fffr:
															// "+arg0[s]+" a
															// "+arg1[s]);
															if (arg0[s].length() < arg1[s].length()) {
																no++;
																result.add(arg0);
																if (oneit.hasNext())
																	arg0 = oneit.next();
																else
																	arg0 = null;
																// System.out.println("here");
																floopy = 1;
																break;

															} else if (arg0[s].length() > arg1[s].length()) {
																result.add(arg1);
																if (twoit.hasNext())
																	arg1 = twoit.next();
																else
																	arg1 = null;
																floopy = 1;
																break;

															} else {
																int res = arg0[s].compareTo(arg1[s]);
																if (res < 0) {
																	result.add(arg0);
																	no++;
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;
																	floopy = 1;
																	break;

																} else if (res > 0) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																}

															}

														}
													}
													if (floopy == 0) {
														no++;
														result.add(arg0);
														if (oneit.hasNext()) {
															arg0 = oneit.next();
														} else {
															arg0 = null;
														}

													}

												}
												if (result.size() == PAGE) {
													// System.out.println("PAGES:
													// "
													// +
													// no);
													// System.out.println("printing");
													check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();
												}

											}

											flag = 1;
											// System.out.println("one: " +
											// oneflag
											// + "
											// s " + sortsize / PAGE);
											// System.out.println("off1 " + off1
											// + "
											// limit " + limit1);
											// System.out.println("off2 " + off2
											// + "
											// limit " + limit2);
											if (oneflag == sortsize / PAGE || off1 + PAGE >= limit1) {

												if (!result.isEmpty()) {
													// check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();

												}
												// check++;
												// System.out.println("here");
												if (arg1 != null) {
													check++;
													String str = "";
													for (String s : arg1) {
														str = str + s + "|";
													}
													str = str.substring(0, str.length() - 1);
													str += '\n';
													nol++;
													wcsv.write(str);
													while (twoit.hasNext()) {
														String[] resadd = (String[]) twoit.next();
														str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
													}
													arg1 = null;
													// System.out.println("off2
													// " +
													// off2
													// + " limit " + limit2);
													if (off2 + PAGE >= limit2 && j == Math.ceil(noinloops) - 1)
														break;
												}
											}
											// System.out.println("two: " +
											// twoflag
											// + "
											// c " + check);
											if (twoflag == sortsize / PAGE || off2 + PAGE >= limit2) {

												if (!result.isEmpty()) {
													// check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();

												}
												// check++;
												if (arg0 != null) {
													check++;
													String str = "";
													for (String s : arg0) {
														str = str + s + "|";
													}
													str = str.substring(0, str.length() - 1);
													str += '\n';
													nol++;
													wcsv.write(str);
													while (oneit.hasNext()) {
														// System.out.print("!hr!");
														String[] resadd = (String[]) oneit.next();
														str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
													}
													arg0 = null;
													// System.out.println("off1
													// " +
													// off1
													// + " limit " + limit1);
													if (off1 + PAGE >= limit1 && j == Math.ceil(noinloops) - 1)
														break;
												}
											}
											// System.out.println("CHECK!!!!!!!!!!!!!!!!!!!!!!!:
											// " + check);
											// System.out.println("LINES: " +
											// nol);
											if (check == (k - 1) * (sortsize / PAGE)
													|| oneflag + twoflag == (k - 1) * (sortsize / PAGE))
												break;

										}
										wcsv.close();
										csv.close();
										csv2.close();

									}

								}

								lineitem = new TreeMap<ArrayList<String>, ArrayList<String>>(
										new Comparator<ArrayList<String>>() {
											@Override
											public int compare(ArrayList<String> arg0, ArrayList<String> arg1) {
												for (int i = 0; i < arg0.size(); i++) {
													if (arg1.get(i).contains(".") || arg0.get(i).contains(".")) {
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

										});
								int filesize = 99000;

								BufferedReader wrcsv = new BufferedReader(
										new FileReader("data/index" + (phases + 1) + "0.csv"));
								String linef = "";
								int fctr = 0;
								int fc = 0;
								while ((linef = wrcsv.readLine()) != null) {
									fctr = 0;
									ArrayList<String> list = new ArrayList<String>();
									BufferedWriter wscsv = new BufferedWriter(
											new FileWriter("data/" + colm + fc + ".csv"));
									while (fctr < filesize && linef != null) {
										if (fctr == 0 || fctr == filesize - 1) {
											String linefs[] = linef.split("\\|");
											list.add(linefs[seqs.get(0)]);
										}
										if ((fc == nrow / filesize && fctr == (nrow % filesize) - 1)) {
											String linefs[] = linef.split("\\|");
											list.add(linefs[seqs.get(0)]);
										}
										wscsv.write(linef);
										wscsv.newLine();
										if (fctr != filesize - 1)
											linef = wrcsv.readLine();
										fctr++;
									}
									if (lineitem.containsKey(list)) {
										ArrayList<String> topa = lineitem.get(list);
										topa.add("data/" + colm + fc + ".csv");
										lineitem.put(list, topa);

									} else {
										ArrayList<String> topa = new ArrayList();
										topa.add("data/" + colm + fc + ".csv");
										lineitem.put(list, topa);
									}
									fc++;
									wscsv.close();
								}

								ArrayList<String> map = new ArrayList<String>();
								String coname = "";

								for (String m : colm) {
									coname = m;
									map.add(coname);

								}
								all.put(map, lineitem);
								lineitem = new TreeMap();

								// System.out.println(all);

							}
							allmap.put(tablename, all);
						}

					} else {

						CreateTable cre = (CreateTable) query;
						String tablename = cre.getTable().toString();
						ArrayList<String> ltcol = new ArrayList();
						// System.gc();
						ltcol.add("orderkey");
						ltcol.add("linenumber");
						ltcol.add("extendedprice");
						ltcol.add("discount");
						ltcol.add("returnflag");
						ltcol.add("commitdate");
						ltcol.add("shipdate");
						ltcol.add("receiptdate");
						ltcol.add("shipmode");
						ArrayList<String> odcol = new ArrayList();
						odcol.add("orderkey");
						odcol.add("custkey");
						odcol.add("orderpriority");
						odcol.add("orderdate");

						List cols = cre.getColumnDefinitions();
						Iterator i = cols.iterator();
						// System.out.println("sdfsd");
						LinkedHashMap<String, ColData> colmap = new LinkedHashMap<String, ColData>();
						int seqcounter = -1;
						while (i.hasNext()) {
							seqcounter++;
							Object col = i.next();
							String arr[] = col.toString().split(" ");
							if (tablename.equals("lineitem")) {
								if (ltcol.contains(arr[0])) {
									ColData colstat = new ColData(seqcounter, arr[1]);
									colmap.put(tablename + '.' + arr[0], colstat);

								}
							} else if (tablename.equals("orders")) {
								if (odcol.contains(arr[0])) {
									ColData colstat = new ColData(seqcounter, arr[1]);
									colmap.put(tablename + '.' + arr[0], colstat);
								}
							} else {
								ColData colstat = new ColData(seqcounter, arr[1]);
								colmap.put(tablename + '.' + arr[0], colstat);
							}
						}
						schema.put(tablename, colmap);

						indexes = new HashMap();

						name = "";
						int pflag = 0;
						int iflag = 0;
						List<Index> index = cre.getIndexes();
						if (index != null) {

							Iterator ind = index.iterator();
							while (ind.hasNext()) {
								// i_seq = new ArrayList();
								Index in = (Index) ind.next();
								if (in.getType().toUpperCase().equals("PRIMARY KEY")) {
									prim = in.getColumnsNames();
									Iterator pk = prim.iterator();
									while (pk.hasNext()) {
										ColData cd = (ColData) colmap.get(tablename + '.' + pk.next());
										pk_seq.add(cd.seq);
									}
									pflag = 1;
								} else {
									l_ind = in.getColumnsNames();
									ArrayList abc1 = new ArrayList();
									name = in.getName();
									Iterator iseq = l_ind.iterator();
									while (iseq.hasNext()) {
										Object ob = iseq.next();
										ColData cd = (ColData) colmap.get(tablename + '.' + ob);
										abc1.add(tablename + "." + ob);
										i_seq.add(cd.seq);
									}
									IndexData id = new IndexData(i_seq, abc1);
									indexes.put(name, id);
									iflag = 1;

								}
							}
							if (tablename.equals("lineitem")) {
								ArrayList<Integer> to = new ArrayList();
								ArrayList<String> ton = new ArrayList();
								to.add(4);

								ton.add("lineitem.returnflag");

								IndexData idm = new IndexData(to, ton);
								indexes.put("index_rt", idm);

								to = new ArrayList();
								ton = new ArrayList();
								to.add(7);
								ton.add("lineitem.receiptdate");
								idm = new IndexData(to, ton);
								indexes.put("index_rec", idm);
								// System.out.println(indexes);
							} else if (tablename.equals("orders")) {
								ArrayList<Integer> to = new ArrayList();
								ArrayList<String> ton = new ArrayList();
								to.add(2);

								ton.add("orders.orderdate");

								IndexData idm = new IndexData(to, ton);
								indexes.put("index_od", idm);

							} else if (tablename.equals("nation") || tablename.equals("region")
									|| tablename.equals("supplier") || tablename.equals("customer")) {
								// assuming no small tables for on-disk

								TreeMap<ArrayList<String>, String[]> data = new TreeMap<ArrayList<String>, String[]>(
										new TreeComparator());

								String read = "";

								BufferedReader csv = new BufferedReader(
										new FileReader("data/" + tablename.toUpperCase() + ".csv"));
								// int check = 0;
								String reads[];
								Iterator<Integer> getpk;
								String patternString = "\\|";
								Pattern pattern = Pattern.compile(patternString);
								Iterator<String> itr;
								while ((read = csv.readLine()) != null) {
									reads = pattern.split(read);
									if (pflag == 1) {
										ArrayList pdata = new ArrayList();

										getpk = pk_seq.iterator();
										while (getpk.hasNext()) {
											pdata.add(reads[getpk.next()]);
										}

										data.put(pdata, reads);

									}
									// check = 1;
								}

								csv.close();
								datamap.put(tablename, data);

								/*
								 * FileOutputStream fileOut = new
								 * FileOutputStream("data/" + tablename +
								 * ".ser"); ObjectOutputStream out = new
								 * ObjectOutputStream(fileOut);
								 * 
								 * out.writeObject(data); out.close();
								 * fileOut.close();
								 */
								data = new TreeMap();

								/*
								 * FileInputStream filein = new
								 * FileInputStream("data/" + tablename +
								 * ".ser"); ObjectInputStream in = new
								 * ObjectInputStream(filein);
								 * 
								 * data = (TreeMap<ArrayList<String>, String[]>)
								 * in.readObject();
								 */

							}

							Iterator<String> indexs = indexes.keySet().iterator();
							// System.out.println(indexes.size());
							while (indexs.hasNext()) {
								String dfg = indexs.next();
								// System.out.println(dfg);
								IndexData db = indexes.get(dfg);
								ArrayList<Integer> seqs = db.seq;
								List<String> colm = db.col;

								// System.out.println(colm+" sdsdf "+seqs);
								int rc = 0;
								int pc = 0;
								String read = "";
								String read2 = "";
								// System.out.println(tablename);
								BufferedReader csv = new BufferedReader(
										new FileReader("data/" + tablename.toUpperCase() + ".csv"));

								int fl = 0;
								int nrow = 0;
								while ((read = csv.readLine()) != null) {
									// System.out.println(read);
									ArrayList<String[]> tempstore = new ArrayList();
									while (rc < MAX_ROWS && read != null) {
										rc++;
										String[] reads = read.split("\\|");
										int ctr = 0;
										String[] reado = new String[colmap.keySet().size()];
										Iterator<String> itr = colmap.keySet().iterator();
										while (itr.hasNext()) {
											ColData cd = colmap.get(itr.next());
											int sq = cd.seq;
											// cd.seq = ctr;
											reado[ctr++] = reads[sq];

										}

										tempstore.add(reado);
										if (rc != MAX_ROWS)
											read = csv.readLine();

									}

									MAX_ROWS = rc;
									// System.out.println(tempstore.get(0)[0]);
									Collections.sort(tempstore, new Comparator<String[]>() {

										@Override
										public int compare(String[] arg0, String[] arg1) {
											for (int s : seqs) {
												if (arg1[s].contains(".") || arg0[s].contains(".")) {

													int cmp;
													if (arg1[s].contains("e") || arg0[s].contains("e")) {
														Double db0 = Double.parseDouble(arg0[s]);
														Double db1 = Double.parseDouble(arg1[s]);
														cmp = db0.compareTo(db1);
														// return cmp;

													} else
														cmp = comp(arg0[s], arg1[s]);

													if (cmp != 0) {

														return cmp;
													}
												} else {
													if (arg0[s].length() < arg1[s].length()) {

														return -1;

													} else if (arg0[s].length() > arg1[s].length()) {

														return 1;

													} else {
														int res = arg0[s].compareTo(arg1[s]);
														if (res != 0) {

															return res;

														}
													}
												}

											}
											return 0;
										}
									});
									rc = 0;
									// sort tempstore here
									pc = 0;
									BufferedWriter wcsv = new BufferedWriter(
											new FileWriter("data/index2" + fl + ".csv"));
									while (pc < MAX_ROWS) {

										// System.out.println(pc);
										String[] towrite = tempstore.get(pc);
										String str = "";
										for (String s : towrite) {
											str = str + s + "|";
										}
										str = str.substring(0, str.length() - 1);
										str += '\n';
										wcsv.write(str);
										nrow++;
										pc++;
										// remove from tempstore
									}
									// MAX_ROWS = PAGE;
									wcsv.close();
									fl++;
								}

								csv.close();

								// System.out.println("nrow: " + nrow);
								MAX_ROWS = 99000;
								NUMROWS = nrow;
								// System.exit(0);
								int numpages = (int) Math.ceil(nrow / PAGE);
								double k = MAX_ROWS / PAGE;
								// System.out.println(k + " " + numpages);
								int phases = (int) Math.ceil(Math.log(numpages / k) / Math.log(k - 1)) + 1;
								BufferedReader csv2 = null;
								System.out.println(" dgd " + phases);
								for (int p = 2; p <= phases; p++) {

									System.out.println(p);
									double sortsize = k * Math.pow((k - 1), (p - 2)) * PAGE;
									double noinloops = (nrow / sortsize) / (k - 1);

									for (int j = 0; j < noinloops; j++) {
										BufferedWriter wcsv = new BufferedWriter(
												new FileWriter("data/index" + (p + 1) + j + ".csv"));

										// System.out.println("in j :" + j);
										int ctr = 0;
										int ctr2 = 0;
										ArrayList<String[]> onetempstore = new ArrayList();
										ArrayList<String[]> twotempstore = new ArrayList();
										Iterator<String[]> oneit = onetempstore.iterator();
										oneit = null;
										Iterator<String[]> twoit = twotempstore.iterator();
										twoit = null;
										ArrayList<String[]> result = new ArrayList();
										int flag = 0, check = 0, oneflag = 0, twoflag = 0;
										double off1 = 0, off2 = 0, limit1 = 0, limit2 = 0;
										String[] arg0 = null, arg1 = null;
										try {
											csv = new BufferedReader(
													new FileReader("data/index" + p + (j * 2) + ".csv"));
											csv2 = new BufferedReader(
													new FileReader("data/index" + p + (j * 2 + 1) + ".csv"));
										} catch (Exception e) {
										}
										while (true) {
											int nol = 0;

											if (flag == 0) {
												off1 = 0;
												limit1 = sortsize;
												off2 = 0;
												limit2 = sortsize;

												if (j == Math.ceil(noinloops) - 1) {
													int row = (int) (nrow - j * sortsize * 2);
													int rows = (int) (row / sortsize);
													if (rows == 0) {
														limit2 = 0;
														limit1 = row;
													} else if (rows == 1) {
														limit1 = sortsize;
														limit2 = row % sortsize;
													} else {
														limit1 = sortsize;
														limit2 = sortsize;
													}
												}
											}
											if (limit2 == 0) {
												while ((read = csv.readLine()) != null) {
													// if (ctr >= off1) {
													// nol++;
													wcsv.write(read);
													wcsv.newLine();
													// }
													// ctr++;
												}
												wcsv.close();
												break;

											}

											if (oneit != null && !oneit.hasNext() && flag == 1
													&& oneflag < sortsize / PAGE && arg0 == null) {
												// System.out.println("inside 1
												// " +
												// oneflag);
												onetempstore.clear();
												off1 += PAGE;
												oneflag++;
												oneit = null;
											}
											if (twoit != null && !twoit.hasNext() && flag == 1
													&& twoflag < sortsize / PAGE && arg1 == null) {
												// System.out.println("twoit
												// nothing");
												twotempstore.clear();
												off2 += PAGE;
												twoflag++;
												twoit = null;
											}
											// System.out.println("ctr1: " + ctr
											// +
											// "ctr2: " + ctr2 + "oneflag: " +
											// oneflag
											// + "twoflag: " + twoflag);
											while (ctr >= off1 && ctr < off1 + PAGE && oneit == null
													&& oneflag < sortsize / PAGE && (read = csv.readLine()) != null) {

												onetempstore.add(read.split("\\|"));
												ctr++;
											}
											while ((ctr2 >= off2 && ctr2 < off2 + PAGE && twoit == null
													&& twoflag < sortsize / PAGE
													&& (read2 = csv2.readLine()) != null)) {

												twotempstore.add(read2.split("\\|"));
												ctr2++;

											}

											// System.out.println("dats size" +
											// onetempstore.size() + " ts " +
											// twotempstore.size()
											// + " o1 " + off1 + " o2 " + off2+"
											// l1
											// "+limit1+" l2 "+limit2);

											if (oneit == null && oneflag < sortsize / PAGE && off1 < limit1) {
												oneit = onetempstore.iterator();
												arg0 = (String[]) oneit.next();
											}
											if (twoit == null && twoflag < sortsize / PAGE && off2 < limit2) {
												twoit = twotempstore.iterator();
												arg1 = (String[]) twoit.next();
											}
											// System.out.println("fffr");
											int floopy;

											int no = 0;
											while (arg0 != null && arg1 != null) {
												floopy = 0;
												if (result.size() < PAGE) {
													for (int s : seqs) {

														if (arg1[s].contains(".") || arg0[s].contains(".")) {

															int cmp;
															if (arg1[s].contains("e") || arg0[s].contains("e")) {
																Double db0 = Double.parseDouble(arg0[s]);
																Double db1 = Double.parseDouble(arg1[s]);
																cmp = db0.compareTo(db1);
																// return cmp;

															} else
																cmp = comp(arg0[s], arg1[s]);

															if (cmp < 0) {
																result.add(arg0);
																no++;
																if (oneit.hasNext())
																	arg0 = oneit.next();
																else
																	arg0 = null;
																floopy = 1;
																break;

															} else if (cmp > 0) {
																result.add(arg1);
																if (twoit.hasNext())
																	arg1 = twoit.next();
																else
																	arg1 = null;
																floopy = 1;
																break;

															}
														}

														else {

															// System.out.println("fffr:
															// "+arg0[s]+" a
															// "+arg1[s]);
															if (arg0[s].length() < arg1[s].length()) {
																no++;
																result.add(arg0);
																if (oneit.hasNext())
																	arg0 = oneit.next();
																else
																	arg0 = null;
																// System.out.println("here");
																floopy = 1;
																break;

															} else if (arg0[s].length() > arg1[s].length()) {
																result.add(arg1);
																if (twoit.hasNext())
																	arg1 = twoit.next();
																else
																	arg1 = null;
																floopy = 1;
																break;

															} else {
																int res = arg0[s].compareTo(arg1[s]);
																if (res < 0) {
																	result.add(arg0);
																	no++;
																	if (oneit.hasNext())
																		arg0 = oneit.next();
																	else
																		arg0 = null;
																	floopy = 1;
																	break;

																} else if (res > 0) {
																	result.add(arg1);
																	if (twoit.hasNext())
																		arg1 = twoit.next();
																	else
																		arg1 = null;
																	floopy = 1;
																	break;

																}

															}

														}
													}
													if (floopy == 0) {
														no++;
														result.add(arg0);
														if (oneit.hasNext()) {
															arg0 = oneit.next();
														} else {
															arg0 = null;
														}

													}

												}
												if (result.size() == PAGE) {
													// System.out.println("PAGES:
													// "
													// +
													// no);
													// System.out.println("printing");
													check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();
												}

											}

											flag = 1;
											// System.out.println("one: " +
											// oneflag
											// + "
											// s " + sortsize / PAGE);
											// System.out.println("off1 " + off1
											// + "
											// limit " + limit1);
											// System.out.println("off2 " + off2
											// + "
											// limit " + limit2);
											if (oneflag == sortsize / PAGE || off1 + PAGE >= limit1) {

												if (!result.isEmpty()) {
													// check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();

												}
												// check++;
												// System.out.println("here");
												if (arg1 != null) {
													check++;
													String str = "";
													for (String s : arg1) {
														str = str + s + "|";
													}
													str = str.substring(0, str.length() - 1);
													str += '\n';
													nol++;
													wcsv.write(str);
													while (twoit.hasNext()) {
														String[] resadd = (String[]) twoit.next();
														str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
													}
													arg1 = null;
													// System.out.println("off2
													// " +
													// off2
													// + " limit " + limit2);
													if (off2 + PAGE >= limit2 && j == Math.ceil(noinloops) - 1)
														break;
												}
											}
											// System.out.println("two: " +
											// twoflag
											// + "
											// c " + check);
											if (twoflag == sortsize / PAGE || off2 + PAGE >= limit2) {

												if (!result.isEmpty()) {
													// check++;
													for (String[] resadd : result) {
														String str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}

														str = str.substring(0, str.length() - 1);

														str += '\n';
														nol++;
														wcsv.write(str);

													}
													result.clear();

												}
												// check++;
												if (arg0 != null) {
													check++;
													String str = "";
													for (String s : arg0) {
														str = str + s + "|";
													}
													str = str.substring(0, str.length() - 1);
													str += '\n';
													nol++;
													wcsv.write(str);
													while (oneit.hasNext()) {
														// System.out.print("!hr!");
														String[] resadd = (String[]) oneit.next();
														str = "";
														for (String s : resadd) {
															str = str + s + "|";
														}
														str = str.substring(0, str.length() - 1);
														str += '\n';
														nol++;
														wcsv.write(str);
													}
													arg0 = null;
													// System.out.println("off1
													// " +
													// off1
													// + " limit " + limit1);
													if (off1 + PAGE >= limit1 && j == Math.ceil(noinloops) - 1)
														break;
												}
											}
											// System.out.println("CHECK!!!!!!!!!!!!!!!!!!!!!!!:
											// " + check);
											// System.out.println("LINES: " +
											// nol);
											if (check == (k - 1) * (sortsize / PAGE)
													|| oneflag + twoflag == (k - 1) * (sortsize / PAGE))
												break;

										}
										wcsv.close();
										csv.close();
										csv2.close();

									}

								}

								lineitem = new TreeMap<ArrayList<String>, ArrayList<String>>(
										new Comparator<ArrayList<String>>() {
											@Override
											public int compare(ArrayList<String> arg0, ArrayList<String> arg1) {

												for (int i = 0; i < arg0.size(); i++) {
													if (arg1.get(i).contains(".") || arg0.get(i).contains(".")) {
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

										});
								int filesize = 99000;

								System.gc();
								Thread.sleep(10000);
								BufferedReader wrcsv = new BufferedReader(
										new FileReader("data/index" + (phases + 1) + "0.csv"));
								BufferedWriter wwww = new BufferedWriter(
										new FileWriter("data/" + tablename + "00.csv"));
								String linef = "";
								int fctr = 0;
								int fc = 0;
								while ((linef = wrcsv.readLine()) != null) {

									fctr = 0;
									ArrayList<String> list = new ArrayList<String>();
									BufferedWriter wscsv = new BufferedWriter(
											new FileWriter("data/" + colm + fc + ".csv"));
									while (fctr < filesize && linef != null) {
										if (tablename.equals("orders")) {
											wwww.write(linef);
											wwww.newLine();
										}
										if (fctr == 0 || fctr == filesize - 1) {
											String linefs[] = linef.split("\\|");
											list.add(linefs[seqs.get(0)]);
										}
										if ((fc == nrow / filesize && fctr == (nrow % filesize) - 1)) {
											String linefs[] = linef.split("\\|");
											list.add(linefs[seqs.get(0)]);
										}
										wscsv.write(linef);
										wscsv.newLine();
										if (fctr != filesize - 1) {
											linef = wrcsv.readLine();

										}

										fctr++;
									}
									if (lineitem.containsKey(list)) {
										ArrayList<String> topa = lineitem.get(list);
										topa.add("data/" + colm + fc + ".csv");
										lineitem.put(list, topa);

									} else {
										ArrayList<String> topa = new ArrayList();
										topa.add("data/" + colm + fc + ".csv");
										lineitem.put(list, topa);
									}
									fc++;
									wscsv.close();
								}
								wwww.close();
								ArrayList<String> map = new ArrayList<String>();
								String coname = "";

								for (String m : colm) {
									coname = m;
									map.add(coname);

								}
								all.put(map, lineitem);
								lineitem = new TreeMap();

								// System.out.println(all);

							}
							Iterator itr = colmap.keySet().iterator();
							int ctr = 0;
							while (itr.hasNext()) {
								ColData cd = colmap.get(itr.next());
								cd.seq = ctr++;
							}

							if (tablename.equals("lineitem") || tablename.equals("orders"))
								allmap.put(tablename, all);
						}

					}
				}

			}
		}

	}

}

// create table r (a int , b int , c string , primary key(a), index cr (c))
// $> select q.a,q.b from (select r.a,r.b from r where r.b>5)q
// Alias: q

// create table r (a int , b int , c string , primary key(a), index cr (c))
// select q.a,q.b,SUM(q.a),COUNT(*),MIN(q.b) from (select r.a,r.b from r where
// r.b>5 )q group by q.a,q.b

// create table r (a int , b int , c string , primary key(a), index cr (c))
// $> select q.a,q.b,SUM(q.a),COUNT(*),MIN(q.b) from (select r.a,r.b from r
// where r.b>5 )q group by q.a,q.b

// create table r ( a int, b int, c string, d date, primary key (a), index dr
// (D));
// select r.a,r.b, count(*) from r where r.d > date('2003-11-11') and r.d<
// date('2016-11-21') group by r.a,r.b;

// select q.b,q.cnt from (select r.b,SUM(r.a) as cnt from r where r.b>2 group by
// r.b)q order by q.cnt
// select q.b,COUNT(q.cnt) from (select r.b,r.c, SUM(r.b) as cnt from r where
// r.b>2 group by r.b,r.c)q group by q.b
// select r.b,r.c, SUM(r.b) as cnt from r where r.b>2 group by r.b,r.c

// create table r (a int , b int , c string , primary key(a), index cr (c))
// $> select m.b,m.cnt2 from (select q.b,COUNT(q.cnt) as cnt2 from (select
// r.b,r.c, SUM(r.b) as cnt from r where r.b>2 group by r.b,r.c)q group by q.b
// order by q.b DESC limit 4)m
/*
 * 
 * 22|7|ghj|2016-05-10 23|7|ghj|2016-05-10 24|5|sdf|2016-05-10
 * 25|5|asr|2016-05-13 27|4|ert|2016-05-10 29|3|dgf|2016-05-10
 */
