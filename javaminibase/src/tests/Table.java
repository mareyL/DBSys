package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;
import java.io.FileNotFoundException;

// Defines a generic Table class which will be used to created all 
// needed tables for our tasks
// 
public class Table {
	public int    col1;
	public int    col2;
	public int    col3;
	public int    col4;
	private Vector table ;
	
	boolean OK = true;
	boolean FAIL = false ; 
		
	public Table () {
	col1    = 0;
	col2    = 0;
	col3    = 0;
	col4    = 0;
		  }
	
	public Table (int _col1, int _col2, int _col3, int _col4) {
	col1    = _col1;
	col2    = _col2;
	col3    = _col3;
	col4    = _col4;
		  }
	
	public int getCol1() {
		return col1;
	}
	
	public int getCol2() {
		return col2;
	}
	
	public int getCol3() {
		return col3;
	}
	
	public int getCol4() {
		return col4;
	}
// The following method, with argument Q, R or S will create the corresponding
// table
	public Vector create_Table(String TableCode) {
		
		 
		table = new Vector() ; 
		
		try
	    {
	        File file1 = new File("../../Output/"+TableCode+".txt");
	        Scanner sc = new Scanner(file1);
	        sc.nextLine();
	        while (sc.hasNextLine()) {
	            String l = sc.nextLine();
	            String[] strArray = l.split(",");
	            table.addElement(new Table(Integer.parseInt(strArray[0]), Integer.parseInt(strArray[1]), Integer.parseInt(strArray[2]),Integer.parseInt(strArray[3])));
	        }
	    }
	    catch (FileNotFoundException e)
	    {
	        System.out.println("File Not Found: "+"../../Output/"+TableCode+".txt");
	    }
		
		return table ; 
		

	
}

}