package dubstep;

import java.util.ArrayList;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.List;

public class Info {
	ArrayList<Integer> pk_seq;
	Map<String, IndexData> indexes;
	List<String> prim;
	ArrayList<String> dtypes,alldt;
	String coln;
	PrimitiveValue min,max;
	
	
	public Info(ArrayList<Integer> pk_seq, Map<String, IndexData> indexes, List<String> prim,ArrayList<String> alldt,ArrayList<String> dtypes, String coln) {
		this.pk_seq = pk_seq;
		this.indexes = indexes;
		this.prim = prim;
		this.alldt = alldt;
		this.dtypes = dtypes;
		this.coln = coln;
		

	}
	public void getvalues(){
		System.out.println("pk_seq  =  "+pk_seq);
		System.out.println("indexes  =  "+indexes);
		System.out.println("prim  =  "+prim);
		System.out.println("alldt  =  "+alldt);
		System.out.println("dtypes  =  "+dtypes);
		
		
	}
	
	public void setmin(PrimitiveValue min){
		this.min=min;
		
	}
	
	public void setmax( PrimitiveValue max){
		this.max=max;
		
	}
	
	

}
