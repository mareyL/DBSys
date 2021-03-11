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



class IEJoinsDriver implements GlobalConst {
	private boolean OK = true;
	private boolean FAIL = false;
	
	private ArrayList<String> result = new ArrayList<String>(); // The requested ouputs
	private ArrayList<String> tables = new ArrayList<String>(); // The tables from where to do the join
	private ArrayList<String> predicate1 = new ArrayList<String>(); // The first predicate 
	private ArrayList<String> predicate2= new ArrayList<String>();  // The second predicate 
	
	private Vector table1 = new Vector();
	private Vector table2 = new Vector();

	private String table1_name;
	private String table2_name;
	
	private int res1;
	private int res2;
	
	private int attribute_1_left;
	private int attribute_1_right;
	private int attribute_2_left;
	private int attribute_2_right;
	
	private int operation_predicate1;
	private int operation_predicate2;
	
	private String symbol_predicate1;  
	private String symbol_predicate2;
	
	private String table_result_left;
	private String table_result_right;
	private String table_predicate1_left;
	private String table_predicate1_right;
	private String table_predicate2_left;
	private String table_predicate2_right;
	
	
	
	  /** Constructor
	   */
	public IEJoinsDriver(String file_name) {
		
		String[] strn = null ;
		try  //read how many raw we will take from file
		{
		    File raw_limits = new File("../../Output/raw_limits.txt");
		    Scanner scn = new Scanner(raw_limits);
		    String ln = scn.nextLine();
		    strn = ln.split(" ");
		}
		catch (FileNotFoundException e)
		{
		    System.out.println("File Not Found Exception !!!!!");
		}
		
		 boolean status = OK;
		 
		 int num_table1 = Integer.parseInt(strn[0]); //size of table1
		 int num_table2 = Integer.parseInt(strn[1]); //size of table2
		
		    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
		    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";
		
		    String remove_cmd = "/bin/rm -rf ";
		    String remove_logcmd = remove_cmd + logpath;
		    String remove_dbcmd = remove_cmd + dbpath;
		    String remove_joincmd = remove_cmd + dbpath;
		
		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		      Runtime.getRuntime().exec(remove_joincmd);
		    }
		    catch (IOException e) {
		      System.err.println (""+e);
		    }

		    
	    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

	    try
	    {
	        File query_file = new File("../../Output/query_" + file_name + ".txt");
	        Scanner sca = new Scanner(query_file);
	        
	        // First line: required outputs = results
	        String line = sca.nextLine();
	        String[] strTab = line.split(" ");
	        for (int i=0 ; i<strTab.length ; i++) { result.add(strTab[i]); }
	        
	        // Line 2: table(s)
	        line = sca.nextLine();
	        strTab = line.split(" ");
	        for (int i=0 ; i<strTab.length ; i++) { tables.add(strTab[i]); }
	        
	        // Line 3: the first predicate
	        line = sca.nextLine();
	        strTab = line.split(" ");
	        for (int i=0 ; i < strTab.length ; i++) { predicate1.add(strTab[i]); }
	        
	        // Line 4: AND (skipped)
	        line = sca.nextLine();  
	        
	        // Line 5: the second predicate
	        line = sca.nextLine();
	        strTab = line.split(" ");
	        for (int i=0 ; i < strTab.length ; i++) { predicate2.add(strTab[i]); }
	    }
	    
	    
	    catch (FileNotFoundException e)
	    {
	        System.out.println("File Not Found: ../../Output/query_" + file_name + ".txt " );
	    }

	     switch (result.get(0)) { // first result
	          case "R_1" : res1 = 1; table_result_left = "R";
	                break;
	          case "R_2" : res1 = 2; table_result_left = "R";
	                break;
	          case "R_3" : res1 = 3; table_result_left = "R";
	                break;
	          case "R_4" : res1 = 4; table_result_left = "R";
	                break;
	          case "S_1" : res1 = 1; table_result_left = "S";
	                break;
	          case "S_2" : res1 = 2; table_result_left = "S";
	                break;
	          case "S_3" : res1 = 3; table_result_left = "S";
	                break;
	          case "S_4" : res1 = 4; table_result_left = "S";
	                break;
	          case "Q_1" : res1 = 1; table_result_left = "Q";
	          		break;
	          case "Q_2" : res1 = 2; table_result_left = "Q";
	          		break;
	          case "Q_3" : res1 = 3; table_result_left = "Q";
	          		break;
	          case "Q_4" : res1 = 4; table_result_left = "Q";
              		break;
	          default : System.out.println("Syntax error in first line");
	                break;
	           }

	    switch (result.get(1)) { // second result
	          case "R_1" : res2 = 1; table_result_right = "R";
	                break;
	          case "R_2" : res2 = 2; table_result_right = "R";
	                break;
	          case "R_3" : res2 = 3; table_result_right = "R";
	                break;
	          case "R_4" : res2 = 4; table_result_right = "R";
	                break;
	          case "S_1" : res2 = 1; table_result_right = "S";
	                break;
	          case "S_2" : res2 = 2; table_result_right = "S";
	                break;
	          case "S_3" : res2 = 3; table_result_right = "S";
	                break;
	          case "S_4" : res2 = 4; table_result_right = "S";
	                break;
	          case "Q_1" : res2 = 1; table_result_right = "Q";
        			break;
	          case "Q_2" : res2 = 2; table_result_right = "Q";
	          		break;
	          case "Q_3" : res2 = 3; table_result_right = "Q";
	          		break;
	          case "Q_4" : res2 = 4; table_result_right = "Q";
	          		break;

	          default : System.out.println("Syntax error in first line");
	                break;
	           }

	    switch (tables.get(0)) { //first table
        case "R" : table1 = new Table().create_Table("r"); table1_name = "R";
              break;
        case "S" : table1 = new Table().create_Table("s"); table1_name = "S";
              break;
        case "Q" : table1 = new Table().create_Table("q"); table1_name = "Q";
              break;
        default : System.out.println("Syntax error in second line");
        		break;
         }
	    
	    switch (tables.get(1)) { //second table
        case "R" : table2 = new Table().create_Table("r"); table2_name = "R";
              break;
        case "S" : table2 = new Table().create_Table("s"); table2_name = "S";
              break;
        case "Q" : table2 = new Table().create_Table("q"); table2_name = "Q";
              break;
        default : System.out.println("Syntax error in second line");
        		break;
         }
	    
	    switch (predicate1.get(0)) { // first attribute of first predicate
	          case "R_1" : attribute_1_left = 1; table_predicate1_left = "R";
	                break;
	          case "R_2" : attribute_1_left = 2; table_predicate1_left = "R";
	                break;
	          case "R_3" : attribute_1_left = 3; table_predicate1_left = "R";
	                break;
	          case "R_4" : attribute_1_left = 4; table_predicate1_left = "R";
	                break;
	          case "S_1" : attribute_1_left = 1; table_predicate1_left = "S";
	                break;
	          case "S_2" : attribute_1_left = 2; table_predicate1_left = "S";
	                break;
	          case "S_3" : attribute_1_left = 3; table_predicate1_left = "S";
	                break;
	          case "S_4" : attribute_1_left = 4; table_predicate1_left = "S";
	                break;
	          case "Q_1" : attribute_1_left = 1; table_predicate1_left = "Q";
        		 	break;
	          case "Q_2" : attribute_1_left = 2; table_predicate1_left = "Q";
        			break;
	          case "Q_3" : attribute_1_left = 3; table_predicate1_left = "Q";
        			break;
	          case "Q_4" : attribute_1_left = 4; table_predicate1_left = "Q";
        			break;

	          default : System.out.println("Syntax error in third line");
	                break;
	           }

	    switch (predicate1.get(2)) { // second attribute of first predicate
	          case "R_1" : attribute_1_right = 1; table_predicate1_right = "R";
	                break;
	          case "R_2" : attribute_1_right = 2; table_predicate1_right = "R";
	                break;
	          case "R_3" : attribute_1_right = 3; table_predicate1_right = "R";
	                break;
	          case "R_4" : attribute_1_right = 4; table_predicate1_right = "R";
	                break;
	          case "S_1" : attribute_1_right = 1; table_predicate1_right = "S";
	                break;
	          case "S_2" : attribute_1_right = 2; table_predicate1_right = "S";
	                break;
	          case "S_3" : attribute_1_right = 3; table_predicate1_right = "S";
	                break;
	          case "S_4" : attribute_1_right = 4; table_predicate1_right = "S";
	                break;
	          case "Q_1" : attribute_1_right = 1; table_predicate1_right = "Q";
              		break;
	          case "Q_2" : attribute_1_right = 2; table_predicate1_right = "Q";
              		break;
	          case "Q_3" : attribute_1_right = 3; table_predicate1_right = "Q";
              		break;
	          case "Q_4" : attribute_1_right = 4; table_predicate1_right = "Q";
        			break;
        			
	          default : System.out.println("Syntax error in third line");
	                break;
	           }

	    switch (predicate1.get(1)) { //operand
	        case "1" : operation_predicate1 = 1; symbol_predicate1 = "<";
	              break;
	        case "2" : operation_predicate1 = 2; symbol_predicate1 = ">";
	          	break;
	        case "3" : operation_predicate1 = 0; symbol_predicate1 = "=";
	          	break;
	        case "4" : operation_predicate1 = 4; symbol_predicate1 = "<=";
	              break;
	        case "5" : operation_predicate1 = 5; symbol_predicate1 = ">=";
	              break;
	        default : System.out.println("Syntax error in third line");
	              break;
	         }

	    switch (predicate2.get(0)) { // first attribute of second predicate
	          case "R_1" : attribute_2_left = 1; table_predicate2_left = "R";
	                break;
	          case "R_2" : attribute_2_left = 2; table_predicate2_left = "R";
	                break;
	          case "R_3" : attribute_2_left = 3; table_predicate2_left = "R";
	                break;
	          case "R_4" : attribute_2_left = 4; table_predicate2_left = "R";
	                break;
	          case "S_1" : attribute_2_left = 1; table_predicate2_left = "S";
	                break;
	          case "S_2" : attribute_2_left = 2; table_predicate2_left = "S";
	                break;
	          case "S_3" : attribute_2_left = 3; table_predicate2_left = "S";
	                break;
	          case "S_4" : attribute_2_left = 4; table_predicate2_left = "S";
	                break;
	          case "Q_1" : attribute_2_left = 1; table_predicate2_left = "Q";
	                break;
	          case "Q_2" : attribute_2_left = 2; table_predicate2_left = "Q";
	                break;
	          case "Q_3" : attribute_2_left = 3; table_predicate2_left = "Q";
	                break;
	          case "Q_4" : attribute_2_left = 4; table_predicate2_left = "Q";
	                break;
	 
	          default : System.out.println("Syntax error in fifth line");
	                break;
	           }

	    switch (predicate2.get(2)) { // second attribute of second predicate
	          case "R_1" : attribute_2_right = 1; table_predicate2_right = "R";
	                break;
	          case "R_2" : attribute_2_right = 2; table_predicate2_right = "R";
	                break;
	          case "R_3" : attribute_2_right = 3; table_predicate2_right = "R";
	                break;
	          case "R_4" : attribute_2_right = 4; table_predicate2_right = "R";
	                break;
	          case "S_1" : attribute_2_right = 1; table_predicate2_right = "S";
	                break;
	          case "S_2" : attribute_2_right = 2; table_predicate2_right = "S";
	                break;
	          case "S_3" : attribute_2_right = 3; table_predicate2_right = "S";
	                break;
	          case "S_4" : attribute_2_right = 4; table_predicate2_right = "S";
	                break;
	          case "Q_1" : attribute_2_right = 1; table_predicate2_right = "Q";
	                break;
	          case "Q_2" : attribute_2_right = 2; table_predicate2_right = "Q";
	                break;
	          case "Q_3" : attribute_2_right = 3; table_predicate2_right = "Q";
	                break;
	          case "Q_4" : attribute_2_right = 4; table_predicate2_right = "Q";
	                break;
	          default : System.out.println("Syntax error in fifth line");
	                break;
	           }

		 switch (predicate2.get(1)) { //operand
	        case "1" : operation_predicate2 = 1; symbol_predicate2 = "<";
	              break;
	        case "2" : operation_predicate2 = 2; symbol_predicate2 = ">";
	          	break;
	        case "3" : operation_predicate2 = 0; symbol_predicate2 = "=";
	          	break;
	        case "4" : operation_predicate2 = 4; symbol_predicate2 = "<=";
	              break;
	        case "5" : operation_predicate2 = 5; symbol_predicate2 = ">=";
	              break;
	        default : System.out.println("Syntax error in third line");
	              break;
	         } 

	
		    // creating the Data relation
		    AttrType [] Dtypes = new AttrType[4];
		    Dtypes[0] = new AttrType (AttrType.attrInteger);
		    Dtypes[1] = new AttrType (AttrType.attrInteger);
		    Dtypes[2] = new AttrType (AttrType.attrInteger);
		    Dtypes[3] = new AttrType (AttrType.attrInteger);

		    //SOS
		    short [] Dsizes = new short [1];
		    Dsizes[0] = 0;

		    Tuple t = new Tuple();
		    try {
		      t.setHdr((short) 4,Dtypes, Dsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    int size = t.size();

		    // inserting the tuple into file "table1"
		    RID             rid;
		    Heapfile        f = null;
		    try {
		      f = new Heapfile("table1.in");
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Heapfile constructor ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    t = new Tuple(size);
		    try {
		      t.setHdr((short) 4, Dtypes, Dsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    for (int i=0; i< num_table1 ; i++) {
		      try {
		    	t.setIntFld(1, ((Table)table1.elementAt(i)).getCol1());
				t.setIntFld(2, ((Table)table1.elementAt(i)).getCol2());
				t.setIntFld(3, ((Table)table1.elementAt(i)).getCol3());
				t.setIntFld(4, ((Table)table1.elementAt(i)).getCol4());
		      }
		      catch (Exception e) {
			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
			status = FAIL;
			e.printStackTrace();
		      }

		      try {
			rid = f.insertRecord(t.returnTupleByteArray());
		      }
		      catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = FAIL;
			e.printStackTrace();
		      }
		    }
		    if (status != OK) {
		      //bail out
		      System.err.println ("*** Error creating relation for table1");
		      Runtime.getRuntime().exit(1);
		    }

		    t = new Tuple();
		    try {
		      t.setHdr((short) 4,Dtypes, Dsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    size = t.size();

		    // inserting the tuple into file "table2"
		    //RID             rid;
		    f = null;
		    try {
		      f = new Heapfile("table2.in");
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Heapfile constructor ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    t = new Tuple(size);
		    try {
		      t.setHdr((short) 4, Dtypes, Dsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }

		    for (int i=0; i<num_table2; i++) {
		      try {
			t.setIntFld(1, ((Table)table2.elementAt(i)).getCol1());
			t.setIntFld(2, ((Table)table2.elementAt(i)).getCol2());
			t.setIntFld(3, ((Table)table2.elementAt(i)).getCol3());
			t.setIntFld(4, ((Table)table2.elementAt(i)).getCol4());
		      }
		      catch (Exception e) {
			System.err.println("*** error in Tuple.setStrFld() ***");
			status = FAIL;
			e.printStackTrace();
		      }

		      try {
			rid = f.insertRecord(t.returnTupleByteArray());
		      }
		      catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = FAIL;
			e.printStackTrace();
		      }
		    }
		    if (status != OK) {
		      //bail out
		      System.err.println ("*** Error creating relation for table2");
		      Runtime.getRuntime().exit(1);
		    }
		  }

		  public boolean runTests() {

		    Query1b();

		    System.out.print ("Finished joins testing"+"\n");


		    return true;
		  }
		  
		  private void Query1b_CondExpr(CondExpr[] expr) {

			    expr[0].next  = null;
			    expr[0].op    = new AttrOperator(operation_predicate1);
			    expr[0].type1 = new AttrType(AttrType.attrSymbol);
			    expr[0].type2 = new AttrType(AttrType.attrSymbol);
			    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
			    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),attribute_1_right);
			    
			    expr[1].next  = null;
			    expr[1].op    = new AttrOperator(operation_predicate2);
			    expr[1].type1 = new AttrType(AttrType.attrSymbol);
			    expr[1].type2 = new AttrType(AttrType.attrSymbol);
			    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3); 
			    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),attribute_2_right); 

			    expr[2] = null ;
			   
			  }
		  
		  public void Query1b() {

			    System.out.print("**********************Query1b strating *********************\n");
			    boolean status = OK;

			    System.out.print 
			      ("Query: \n"
			    		  + "  SELECT  " + table_result_left + "." + res1 + ", " + table_result_right + "." + res2 + "\n"
			    		  + "  FROM  " + table1_name + ", " + table2_name + "\n"
					      + "  WHERE  " + table_predicate1_left + ".col" + attribute_1_left + " " + symbol_predicate1 + " " + table_predicate1_right + ".col" + attribute_1_right + "\n"
			    		  + "  AND  " + table_predicate2_left + ".col" + attribute_2_left + " " + symbol_predicate2 + " " + table_predicate2_right + ".col" + attribute_2_right + "\n");
			    
			    IndexType b_index = new IndexType (IndexType.B_Index);
			    
			    CondExpr[] outFilter = new CondExpr[3];
			    outFilter[0] = new CondExpr();
			    outFilter[1] = new CondExpr();
			    outFilter[2] = new CondExpr();

			    Query1b_CondExpr(outFilter);
			    Tuple t = new Tuple();
			    t = null;

			    AttrType [] Dtypes = new AttrType[4];
			    Dtypes[0] = new AttrType (AttrType.attrInteger);
			    Dtypes[1] = new AttrType (AttrType.attrInteger);
			    Dtypes[2] = new AttrType (AttrType.attrInteger);
			    Dtypes[3] = new AttrType (AttrType.attrInteger);

			    AttrType [] Dtypes2 = new AttrType[3];
			    Dtypes2[0] = new AttrType (AttrType.attrInteger);
			    Dtypes2[1] = new AttrType (AttrType.attrInteger);
			    Dtypes2[2] = new AttrType (AttrType.attrInteger);

			    
			    //SOS
			    short [] Dsizes = new short[1];
			    Dsizes[0] = 0; 
			    
			    FldSpec [] Dprojection = {
			    	       new FldSpec(new RelSpec(RelSpec.outer), res1),
			    	       new FldSpec(new RelSpec(RelSpec.outer), attribute_1_left),
			    	       new FldSpec(new RelSpec(RelSpec.outer), attribute_2_left),
			    	    };
			    
			    FldSpec [] proj = new FldSpec[2];
			    proj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			    proj[1] = new FldSpec(new RelSpec(RelSpec.innerRel), res2);

			    iterator.Iterator am1 = null;
			    iterator.Iterator am2 = null;


		       
			    Tuple tt = new Tuple();
			    try {
			      tt.setHdr((short) 4, Dtypes, Dsizes);
			    }
			    catch (Exception e) {
			      status = FAIL;
			      e.printStackTrace();
			    }

			    int sizett = tt.size();
			    tt = new Tuple(sizett);
			    try {
			      tt.setHdr((short) 4, Dtypes, Dsizes);
			    }
			    catch (Exception e) {
			      status = FAIL;
			      e.printStackTrace();
			    }
			    Heapfile        f = null;
			    try {
			      f = new Heapfile("table1.in");
			    }
			    catch (Exception e) {
			      status = FAIL;
			      e.printStackTrace();
			    }


		          Scan scan = null;
		          try {
		            scan = new Scan(f);
		          }
		          catch (Exception e) {
		            status = FAIL;
		            e.printStackTrace();
		            Runtime.getRuntime().exit(1);
		          }

		          // create the index file
		          BTreeFile btf = null;
		          try {
		            btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
		          }
		          catch (Exception e) {
		            status = FAIL;
		            e.printStackTrace();
		            Runtime.getRuntime().exit(1);
		          }

		          RID rid = new RID();
		          int key =0;
		          Tuple temp = null;

		          try {
		            temp = scan.getNext(rid);
		          }
		          catch (Exception e) {
		            status = FAIL;
		            e.printStackTrace();
		          }
		          while ( temp != null) {
		            tt.tupleCopy(temp);

		            try {
		      	key = tt.getIntFld(1);
		            }
		            catch (Exception e) {
		      	status = FAIL;
		      	e.printStackTrace();
		            }

		            try {
		      	btf.insert(new IntegerKey(key), rid);
		            }
		            catch (Exception e) {
		      	status = FAIL;
		      	e.printStackTrace();
		            }

		            try {
		      	temp = scan.getNext(rid);
		            }
		            catch (Exception e) {
		      	status = FAIL;
		      	e.printStackTrace();
		            }
		          }

		          // close the file scan
		          scan.closescan();
		          
		          
		          System.out.print ("After Building btree index on sailors.sid.\n\n");
		          try {
		            am1 = new IndexScan ( b_index, "table1.in",
		      			   "BTreeIndex", Dtypes, Dsizes, 4, 3,
		      			   Dprojection, null, 1, false);
		          }

		          catch (Exception e) {
		            System.err.println ("*** Error creating scan for Index scan");
		            System.err.println (""+e);
		            Runtime.getRuntime().exit(1);
		          }
		          
		          try {
			            am2 = new IndexScan ( b_index, "table2.in",
			      			   "BTreeIndex", Dtypes, Dsizes, 4, 3,
			      			   Dprojection, null, 1, false);
			          }

			          catch (Exception e) {
			            System.err.println ("*** Error creating scan for Index scan");
			            System.err.println (""+e);
			            Runtime.getRuntime().exit(1);
			          }
		          
		          IEJoin iej = null;
		          try {
		            iej = new IEJoin (Dtypes2, 3, Dsizes,
		      				  Dtypes, 4, Dsizes,
		      				  10,
		      				  am1, am2 , "table2.in",
		      				  outFilter, null, proj, 2);
		          }
		          catch (Exception e) {
		            System.err.println ("*** Error preparing for nested_loop_join");
		            System.err.println (""+e);
		            e.printStackTrace();
		            Runtime.getRuntime().exit(1);
		          }

		          t = null;
		          try {
		            while ((t = iej.get_next()) != null) {
		              t.print(Dtypes2);
		            }
		          }
		          catch (Exception e) {
		            System.err.println (""+e);
		            e.printStackTrace();
		            Runtime.getRuntime().exit(1);
		          }
		  }
	}

public class IEJoinTest {
	public static void main(String argv[]) {
		boolean sortstatus;

		System.out.println("querry 2c");
	    IEJoinsDriver jjoin = new IEJoinsDriver("2c");

	    sortstatus = jjoin.runTests();
	    if (sortstatus != true) {
	      System.out.println("Error ocurred during join tests");
	    }
	    else {
	      System.out.println("join tests completed successfully");
	    }
	    
		System.out.println("querry 2c_1");
	    IEJoinsDriver jjoin2 = new IEJoinsDriver("2c_1");

	    sortstatus = jjoin2.runTests();
	    if (sortstatus != true) {
	      System.out.println("Error ocurred during join tests");
	    }
	    else {
	      System.out.println("join tests completed successfully");
	    }
	    
	    
		System.out.println("querry 2c_2");
	    IEJoinsDriver jjoin3 = new IEJoinsDriver("2c_2");

	    sortstatus = jjoin3.runTests();
	    if (sortstatus != true) {
	      System.out.println("Error ocurred during join tests");
	    }
	    else {
	      System.out.println("join tests completed successfully");
	    }
	}

}
