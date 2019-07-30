package main

import (
    "fmt"
    "os"
    "strconv"
    "math"
	"encoding/csv"
    "sync"
)

var cutoff = 1000
var copy_cutoff =5


//CODE FOR ROUND FUNCTION WAS TAKEN FROM https://gist.github.com/DavidVaini/10308388
func Round(val float64, roundOn float64, places int ) (newVal float64) {
    var round float64
    pow := math.Pow(10, float64(places))
    digit := pow * val
    _, div := math.Modf(digit)
    if div >= roundOn {
        round = math.Ceil(digit)
    } else {
        round = math.Floor(digit)
    }
    newVal = round / pow
    return}

type Grid struct {maxlong,minlong,maxlat,minlat float64}

type PopStats struct{
    curr_pop int
    tot_pop int}

type CensusGroup struct {
	population int
	latitude, longitude float64}

type Cooridnate struct {
    row int
    col int}

type SafeInt struct{
    elem int
    mux sync.Mutex
}

func getCoordinates(grid Grid,xdim int,ydim int,currcensus CensusGroup) (Cooridnate) {
    var rowlen, colen float64
    var currrow,currcol float64
    var therow,thecol int
    

    rowlen = (grid.maxlat - grid.minlat)/float64(ydim)
    colen = (grid.maxlong-grid.minlong)/float64(xdim)

    currrow = (currcensus.latitude - grid.minlat)/rowlen
    currcol = (currcensus.longitude - grid.minlong)/colen

    //tie break will automatically be assigned to north
    if currrow-Round(currrow,1,0)>=0 {
        currrow++
    }

    currcol++


    therow = int(currrow)
    thecol = int(currcol)

    if therow==ydim+1{
        therow--
    }

    if thecol==xdim+1{
        thecol--
    }

    var retcord Cooridnate
    retcord.row = therow
    retcord.col = thecol

    return retcord}

func InGrid (grid Grid,west int, south int, east int, north int, xdim int, ydim int,currcensus CensusGroup) (bool) {

    var mycoordinates Cooridnate
    mycoordinates = getCoordinates(grid,xdim,ydim,currcensus)
    var retbool bool
    retbool = false

    if mycoordinates.row>=south && mycoordinates.row<=north && mycoordinates.col>=west && mycoordinates.col<=east {
        retbool = true
    }

    return retbool}

func Query1 (grid Grid,west int, south int, east int, north int, xdim int, ydim int,censusData []CensusGroup) (PopStats) {
    var ret_pop,tot_pop int
    ret_pop = 0
    tot_pop = 0
    stats := PopStats{curr_pop:0,tot_pop:0}


    for i:= 0;i<len(censusData);i++ {
        tot_pop = tot_pop + censusData[i].population
        if InGrid(grid,west,south,east,north,xdim,ydim,censusData[i]) {
            ret_pop = ret_pop + censusData[i].population  
        }

    }

    stats.tot_pop = tot_pop
    stats.curr_pop = ret_pop
    return stats}

func Query2 (grid Grid,west int, south int, east int, north int, xdim int, ydim int,censusData []CensusGroup) (PopStats) {

    stats := PopStats{curr_pop:0,tot_pop:0}

    if (len(censusData)<cutoff && len(censusData)>1){
        stats = Query1(grid,west,south,east,north,xdim,ydim,censusData)
    }

    if len(censusData)>1 && len(censusData)>cutoff {

        var mid int
        mid= len(censusData)/2

        statsleft  := make(chan PopStats)
        go func() {
            statsleft<- Query2(grid,west,south,east,north,xdim,ydim,censusData[:mid])
        }()

        statsright := Query2(grid,west,south,east,north,xdim,ydim,censusData[mid:])
        retstats := <- statsleft

        stats.curr_pop = statsright.curr_pop + retstats.curr_pop
        stats.tot_pop = statsright.tot_pop + retstats.tot_pop
    }

    return stats}

func Query3(grid [][]int,west int,south int,east int,north int,xdim int,ydim int) (PopStats){
    var retstats PopStats
    retstats.curr_pop = 0
    retstats.tot_pop = 0

    retstats.curr_pop = getgridstats(grid,west,south,east,north,ydim,xdim)
    retstats.tot_pop = grid[ydim-1][xdim-1]

    return retstats}


func Query5(grid [][]SafeInt,west int,south int,east int,north int,xdim int,ydim int) (PopStats){
    var retstats PopStats
    retstats.curr_pop = 0
    retstats.tot_pop = 0

    retstats.curr_pop = safegetgridstats(grid,west,south,east,north,ydim,xdim)
    retstats.tot_pop = grid[ydim-1][xdim-1].elem

    return retstats}

func ParseCensusData(fname string) ([]CensusGroup, error) {
	file, err := os.Open(fname)
    if err != nil {
		return nil, err
    }
    defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	censusData := make([]CensusGroup, 0, len(records))

    for _, rec := range records {
        if len(rec) == 7 {
            population, err1 := strconv.Atoi(rec[4])
            latitude, err2 := strconv.ParseFloat(rec[5], 64)
            longitude, err3 := strconv.ParseFloat(rec[6], 64)
            if err1 == nil && err2 == nil && err3 == nil {
                latpi := latitude * math.Pi / 180
                latitude = math.Log(math.Tan(latpi) + 1 / math.Cos(latpi)) 
                censusData = append(censusData, CensusGroup{population, latitude, longitude})
            }
        }
    }

	return censusData, nil}

func prepareGrid2(censusData []CensusGroup) (Grid) {
    
    retgrid := Grid{maxlat:censusData[0].latitude,minlat:censusData[0].latitude,
            maxlong:censusData[0].longitude,minlong:censusData[0].longitude}

    if (len(censusData)<cutoff && len(censusData)>1){
        ngrid := prepareGrid1(censusData)
        retgrid = ngrid
        // fmt.Println("Cutoff was called.")
    }

    if len(censusData)>1 && len(censusData)>cutoff {

        var mid int
        // fmt.Println("Yes I was called.")
        mid= len(censusData)/2
        
        gridleft  := make(chan Grid)
        go func() {
            gridleft<- prepareGrid2(censusData[:mid])
        }()

        gridright := prepareGrid2(censusData[mid:])
        megrid:= <-gridleft

        retgrid.maxlat = math.Max(gridright.maxlat,(megrid).maxlat)
        retgrid.maxlong = math.Max(gridright.maxlong,(megrid).maxlong)
        retgrid.minlat = math.Min(gridright.minlat,(megrid).minlat)
        retgrid.minlong = math.Min(gridright.minlong,(megrid).minlong)
        }
    // fmt.Println("CORNER FINDING 2 RETURNED")
    return retgrid}

func prepareGrid1(censusData []CensusGroup) (Grid) {
    grid := Grid{maxlat:censusData[0].latitude,minlat:censusData[0].latitude,
        maxlong:censusData[0].longitude,minlong:censusData[0].longitude}

    for i:=0;i<len(censusData);i++ {
            if censusData[i].latitude>grid.maxlat{
                    grid.maxlat = censusData[i].latitude
            }

            if censusData[i].latitude<grid.minlat{
                    grid.minlat = censusData[i].latitude
            }

            if censusData[i].longitude>grid.maxlong{
                    grid.maxlong = censusData[i].longitude
            }

            if censusData[i].longitude<grid.minlong{
                    grid.minlong = censusData[i].longitude
            }
        }

    return grid}

func prepareGrid3(gridhelper Grid,censusData []CensusGroup,xdim int,ydim int) ([][]int) {

    grid := make([][]int, ydim)
    for i := range grid {
        grid[i] = make([]int, xdim)
    }

    for i:=0;i<len(censusData);i++{
        currcord := getCoordinates(gridhelper,xdim,ydim,censusData[i])
        grid = addtogrid(grid,currcord.row,currcord.col,censusData[i].population,xdim,ydim)
    }

    //NOW WE WILL PREPROCESS THE DATA
    // top edge
    

    return grid}

func prepareGrid4(gridhelper Grid,censusData []CensusGroup,xdim int,ydim int) ([][]int) {

    if len(censusData)<cutoff{
        retgrid:=prepareGrid3(gridhelper,censusData,xdim,ydim)
        return retgrid
    }
    
    var mid int
    mid= len(censusData)/2

    gridleft  := make(chan [][]int)

    go func() {
        gridleft<- prepareGrid4(gridhelper,censusData[:mid],xdim,ydim)
    }()
    
    rgrid := prepareGrid4(gridhelper,censusData[mid:],xdim,ydim)
    lgrid:=<-gridleft
    retgrid := sequentialmerge(rgrid,lgrid,0,len(rgrid))

    return retgrid}

func prepareGrid5(gridhelper Grid,censusData []CensusGroup,xdim int,ydim int) ([][]SafeInt) {
	
	safegrid := make([][]SafeInt, ydim)
    for i := range safegrid {
        safegrid[i] = make([]SafeInt, xdim)
    }

    for i:=0;i<len(censusData);i++{
        currcord := getCoordinates(gridhelper,xdim,ydim,censusData[i])
        go safeaddtogrid(safegrid,currcord.row,currcord.col,censusData[i].population,xdim,ydim)
    }

    return safegrid
}


func safeaddtogrid(safegrid [][]SafeInt,row int,col int,val int,xdim int,ydim int){
    var rowindex,colindex int
    if row!=ydim{
    rowindex = (row-ydim)*-1
    }
	if row==ydim{
        rowindex = 0
    }
	colindex = col-1
	safegrid[rowindex][colindex].mux.Lock()
	safegrid[rowindex][colindex].elem = safegrid[rowindex][colindex].elem + val
	safegrid[rowindex][colindex].mux.Unlock()
    }

func smarterstep2locks(grid [][]SafeInt,xdim int,ydim int) ([][]SafeInt) {

    for i:=1;i<xdim;i++{
        grid[0][i].elem += grid[0][i-1].elem
    }

    //left edge
    for i:=1;i<ydim;i++{
        grid[i][0].elem += grid[i-1][0].elem
    }

    for r:=1;r<ydim;r++{
        for c:=1;c<xdim;c++{
            grid[r][c].elem = grid[r][c].elem+grid[r-1][c].elem+ grid[r][c-1].elem - grid[r-1][c-1].elem
        }
    }

    return grid}


func smarterstep2(grid [][]int,xdim int,ydim int) ([][]int) {

    for i:=1;i<xdim;i++{
        grid[0][i] += grid[0][i-1]
    }

    //left edge
    for i:=1;i<ydim;i++{
        grid[i][0] += grid[i-1][0]
    }

    for r:=1;r<ydim;r++{
        for c:=1;c<xdim;c++{
            grid[r][c] = grid[r][c]+grid[r-1][c]+ grid[r][c-1] - grid[r-1][c-1]
        }
    }

    return grid}


func safegetgridstats (grid [][]SafeInt,w int,s int,e int,n int,rows int ,cols int) (int) {
    if s!=rows{
    s = (s-rows)*-1
    }
    if s==rows{
        s = 0
    }
    if n!=rows{
        n = (n-rows)*-1
    }
    if n==rows{
        n = 0
    }
    e=e-1
    w=w-1
    var atpright,lbleft,aleftuleft int
    atpright = 0
    lbleft = 0
    aleftuleft = 0
    if n!=0{
        atpright = grid[n-1][e].elem
    } 
    if w!=0 {
        lbleft = grid[s][w-1].elem
    }
    if n!=0 && w!=0{
        aleftuleft = grid[n-1][w-1].elem
    }
    btright := grid[s][e].elem
    var ans int
    ans = btright - atpright - lbleft + aleftuleft
    return ans}



func getgridstats (grid [][]int,w int,s int,e int,n int,rows int ,cols int) (int) {
    if s!=rows{
    s = (s-rows)*-1
    }
    if s==rows{
        s = 0
    }
    if n!=rows{
        n = (n-rows)*-1
    }
    if n==rows{
        n = 0
    }
    e=e-1
    w=w-1
    var atpright,lbleft,aleftuleft int
    atpright = 0
    lbleft = 0
    aleftuleft = 0
    if n!=0{
        atpright = grid[n-1][e]
    } 
    if w!=0 {
        lbleft = grid[s][w-1]
    }
    if n!=0 && w!=0{
        aleftuleft = grid[n-1][w-1]
    }
    btright := grid[s][e]
    var ans int
    ans = btright - atpright - lbleft + aleftuleft
    return ans}

func addtogrid(grid [][]int,row int,col int,val int,xdim int,ydim int) ([][]int){
    var rowindex,colindex int
    if row!=ydim{
    rowindex = (row-ydim)*-1
    }
	if row==ydim{
        rowindex = 0
    }
	colindex = col-1
	grid[rowindex][colindex]=grid[rowindex][colindex] + val
    return grid}

func sequentialmerge(grid1 [][]int,grid2 [][]int,l int,h int) ([][]int){

    csize := len(grid2[0])
    for r:=l;r<h;r++{
        for c:=0;c<csize;c++{
            grid1[r][c] += grid2[r][c]
        }
    }
    return grid1}

func parallelmerge(grid1 [][]int,grid2 [][]int,l int,h int) ([][]int){

    if h-l<copy_cutoff {
        return  sequentialmerge(grid1,grid2,l,h)
    }    
    mid:= (l+h)/2
    gridleft  := make(chan [][]int)
    go func() {
        gridleft<-parallelmerge(grid1,grid2,l,mid)
    }()

    rgrid := parallelmerge(grid1,grid2,mid,h)
    <-gridleft
    return rgrid}



func parallelprefixcolumn(array [][]SafeInt,parent chan SafeInt,col int) {

	if (len(array))>1 {
		mid := len(array)/2
		left := make(chan SafeInt)
		right := make(chan SafeInt)
		go parallelprefixcolumn(array[:mid],left,col)
		go parallelprefixcolumn(array[mid:],right,col)

		leftSum:= (<-left).elem
		var sendelem SafeInt
		sendelem.elem =(<-right).elem+ leftSum

		parent<- sendelem

		fromLeft := <-parent
		var toright SafeInt
		toright.elem = (fromLeft).elem + (leftSum)
		left <- fromLeft
		right <- toright
		var topar SafeInt
		topar.elem = (<-left).elem + (<-right).elem
		parent<- topar
	} else {
		parent <- array[0][col]
		array[0][col].elem += (<-parent).elem
		var temp SafeInt
		parent <- temp	
	}

}


func parallelprefixrow(array []SafeInt,parent chan SafeInt) {

	if (len(array))>1 {
		mid := len(array)/2
		left := make(chan SafeInt)
		right := make(chan SafeInt)
		go parallelprefixrow(array[:mid],left)
		go parallelprefixrow(array[mid:],right)

		leftSum:= (<-left).elem
		var sendelem SafeInt
		sendelem.elem =(<-right).elem+ leftSum

		parent<- sendelem

		fromLeft := <-parent
		var toright SafeInt
		toright.elem = (fromLeft).elem + (leftSum)
		left <- fromLeft
		right <- toright
		var topar SafeInt
		topar.elem = (<-left).elem + (<-right).elem
		parent<- topar
	} else {
		var topar SafeInt
		topar.elem = array[0].elem
		parent <-topar 
		array[0].elem += (<-parent).elem
		var temp SafeInt
		parent <- temp	
	}

}

func prefixarraycol(array [][]SafeInt) {

	for i:=0;i<len(array[0]);i++{
		ch := make(chan SafeInt)
		go parallelprefixcolumn(array,ch,i)
		var temp SafeInt
		temp.elem =0
		<- ch
		ch <-temp
		<- ch
	}

}


func prefixarrayrow(array [][]SafeInt) {

	for i:=0;i<len(array);i++{
		ch := make(chan SafeInt)
		go parallelprefixrow(array[i],ch)
		var temp SafeInt
		temp.elem =0
		<- ch
		ch <-temp
		<- ch
	}
}


func completeparallelprefix(array [][]SafeInt) {

	prefixarrayrow(array)
	prefixarraycol(array)
}


func main() {
    
	if len(os.Args) < 4 {
        fmt.Println(os.Args[1])
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}

	fname, ver := os.Args[1], os.Args[4]
    xdim, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
    ydim, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		return
	}
	censusData, err := ParseCensusData(fname)
	if err != nil {
		fmt.Println(err)
		return
	}
    var grid1,grid2 Grid


    var grid3,grid4 [][]int
    var grid5 [][]SafeInt
    var grid6 [][]SafeInt
    var stats PopStats

    // Some parts may need no setup code
    switch ver {
    case "-v1":
        grid1 = prepareGrid1(censusData)
    case "-v2":
        grid2 = prepareGrid2(censusData)
    case "-v3":
        grid3helper:=prepareGrid1(censusData)
        grid3 = prepareGrid3(grid3helper,censusData,xdim,ydim)
        grid3 = smarterstep2(grid3,xdim,ydim)
    case "-v4":
        //Parallel corner finding
        grid4helper:= prepareGrid2(censusData)
        //Parallel update grid
        grid4 = prepareGrid4(grid4helper,censusData,xdim,ydim)
        //Sequential step 2
        grid4 = smarterstep2(grid4,xdim,ydim)
    case "-v5":
		grid5helper:= prepareGrid2(censusData)
        //Parallel update grid
        grid5 = prepareGrid5(grid5helper,censusData,xdim,ydim)
        //Sequential step 2
        grid5 = smarterstep2locks(grid5,xdim,ydim)	        
    case "-v6":
        grid6helper:= prepareGrid2(censusData)
        //Parallel update grid
        grid6 = prepareGrid5(grid6helper,censusData,xdim,ydim)
        //Sequential step 2
        completeparallelprefix(grid6)
    default:
        fmt.Println("Invalid version argument")
        return
    }

    for {
        var west, south, east, north int
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }
   
        
        var population int
        var percentage float64
       
        switch ver {
        case "-v1":
            stats = Query1(grid1,west,south,east,north,xdim,ydim,censusData)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
        case "-v2":
            stats = Query2(grid2,west,south,east,north,xdim,ydim,censusData)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
        case "-v3":
            stats = Query3(grid3,west,south,east,north,xdim,ydim)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
        case "-v4":
            stats = Query3(grid4,west,south,east,north,xdim,ydim)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
            // YOUR QUERY CODE FOR PART 4
        case "-v5":
            stats = Query5(grid5,west,south,east,north,xdim,ydim)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
            // YOUR QUERY CODE FOR PART 5
        case "-v6":
            stats = Query5(grid6,west,south,east,north,xdim,ydim)
            population = stats.curr_pop
            percentage = (float64(stats.curr_pop)/float64(stats.tot_pop))*100
            // YOUR QUERY CODE FOR PART 6
        }

        fmt.Printf("%v %.2f%%\n", population, percentage)
    }
}
