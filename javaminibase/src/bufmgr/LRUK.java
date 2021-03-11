package bufmgr;

import diskmgr.*;
import global.*;
import java.time.*;

public class LRUK extends Replacer {

	  /**
	   * private field
	   * An array to hold number of frames in the buffer pool
	   */
	  private int  frames[];
	 
	  /**
	   * private field
	   * number of frames used
	   */   
	  private int  nframes;

	  /**
	   * private field
	   * number k
	   */   
	  private int k = 3;
	  
	  /**
	   * private field 
	   * An array to account for the Historic of pages in the buffer
	   */
	  private long[] hist[] ; 
	  
	  /**
	   * private field 
	   * An array to account for the time of last reference for the pages 
	   * in the buffer
	   */
	  private long last[] ; 
	  
	  /**
	   * An integer corresponding to the correlated reference period
	   */
	  private long correlPeriod = 5000 ; 
	  
	 
	  
	  /**
	   * Calling super class the same method
	   * Initializing the frames[] with number of buffer allocated
	   * by buffer manager
	   * set number of frame used to zero
	   *
	   * @param	mgr	a BufMgr object
	   * @see	BufMgr
	   * @see	Replacer
	   */
	    public void setBufferManager( BufMgr mgr )
	     {
	        super.setBufferManager(mgr);
		frames = new int [ mgr.getNumBuffers() ];
		nframes = 0;
		hist = new long[mgr.getNumBuffers()][k] ;
		last = new long[mgr.getNumBuffers()] ; 
	     }

	/* public methods */

	  /**
	   * Class constructor
	   * Initializing frames[] pinter = null.
	   */
	    public LRUK(BufMgr mgrArg, int k)
	    {
	      super(mgrArg);
	      frames = null;
	      this.k = k;
	      this.hist = null;
	      this.last = null;
	    }
	    
	    
	    /**
	     * setter and getter for the last access list
	     */
	    public void setLast(int frameNo,long time  ) {this.last[frameNo] = time ;}
	    public long getLast(int frameNo) {return this.last[frameNo];}
	    
	    /**
	     * setter and getter for the hist list
	     */
	    public void setHist(int frameNo, int position, long time  ) {this.hist[frameNo][position] = time ; }
	    public long getHist(int frameNo, int position) {return this.hist[frameNo][position];}
	    
	    
	    public int[] getFrames() {return frames;}
	    /**
		   * This pushes the given frame to the end of the list.
		   * @param frameNo	the frame number
		   */
		  private void update(int frameNo)
		  {
		     int index;
		     long time = System.currentTimeMillis() ;
		     for ( index=0; index < nframes; ++index )
		        if ( frames[index] == frameNo ) {
		        	
		        	if(time - getLast(frameNo)> this.correlPeriod) {
		        		long corelPeriodRefPage = getLast(frameNo) - getHist(frameNo,0 );
		        		for(int i =1 ; i < this.k ; i++) {
		        			setHist(frameNo, i, getHist(frameNo, i-1) + corelPeriodRefPage);
		        		}
		        		setHist(frameNo, 0, time) ;
		        		setLast(frameNo, time) ;
		        	}
		        	else {setLast(frameNo, time);}
		        
		        }
		       
		  }
		  
		  
	  /**
	   * calll super class the same method
	   * pin the page in the given frame number 
	   * move the page to the end of list  
	   *
	   * @param	 frameNo	 the frame number to pin
	   * @exception  InvalidFrameNumberException
	   */
	 public void pin(int frameNo) throws InvalidFrameNumberException
	 {
	    super.pin(frameNo);
	    update(frameNo);
	    
	 }

	  /**
	   * Finding a free frame in the buffer pool
	   * or choosing a page to replace using LRUK policy
	   *
	   * @return 	return the frame number
	   *		return -1 if failed
	   */
	 
	 
	 public int pick_victim( ) throws BufferPoolExceededException
	 {
	
	   int numBuffers = mgr.getNumBuffers();
	   int frame;
	   long time = System.currentTimeMillis();
	   long min = time;
	   int victim_index = 0 ;
	   boolean victim_found = false ; 
	   for(frame = 0 ; frame< numBuffers ; frame++){
		   if(time - getLast(frame) > correlPeriod && getHist(frame, this.k-1)< min && state_bit[frame].state != Pinned) {
			   victim_index = frame ; 
			   min = getHist(victim_index, this.k-1);
			   victim_found = true ; 
			   
			   }
	   }
	   
	   
	   if(victim_found) {
		   state_bit[victim_index].state = Pinned;
		   (mgr.frameTable())[victim_index].pin();
		   return victim_index ;
       }
	   else {throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");}
	   
	   }
	
	 
	 
	 
	  /**
	   * get the page replacement policy name
	   *
	   * @return	return the name of replacement policy used
	   */  
	 public String name() { return "LRUK"; }
	 
	  /**
	   * print out the information of frame usage
	   */  
	 public void info()
	 {
	    super.info();

	    System.out.print( "LRUK REPLACEMENT");
	    
	    for (int i = 0; i < nframes; i++) {
	        if (i % 5 == 0)
		System.out.println( );
		System.out.print( "\t" + frames[i]);
	        
	    }
	    System.out.println();
	 }
}