<?php
/**
 * @desc elastic search helper
 */
class ElasticSearch {

    public $index;

    function __construct($server = 'http://cdev.nokta.com:9200') {
        $this->server = $server;
    }

    function call($path, $http = array()) {
        echo "GETTING ".$this->server . '/' . $this->index . '/' . $path, NULL, stream_context_create(array('http' => $http));
        if (!$this->index)
            throw new Exception('$this->index needs a value');
        return json_decode(file_get_contents($this->server . '/' . $this->index . '/' . $path, NULL, stream_context_create(array('http' => $http))));
    }

    //curl -X PUT http://localhost:9200/{INDEX}/
    function create() {
        $this->call(NULL, array('method' => 'PUT'));
    }

    //curl -X DELETE http://localhost:9200/{INDEX}/
    function drop() {
        $this->call(NULL, array('method' => 'DELETE'));
    }

    //curl -X GET http://localhost:9200/{INDEX}/_status
    function status() {
        return $this->call('_status');
    }

    //curl -X GET http://localhost:9200/{INDEX}/{TYPE}/_count -d {matchAll:{}}
    function count($type) {
        return $this->call($type . '/_count', array('method' => 'GET', 'content' => '{ matchAll:{} }'));
    }

    //curl -X PUT http://localhost:9200/{INDEX}/{TYPE}/_mapping -d ...
    function map($type, $data) {
        return $this->call($type . '/_mapping', array('method' => 'PUT', 'content' => $data));
    }

    //curl -X PUT http://localhost:9200/{INDEX}/{TYPE}/{ID} -d ...
    function add($type, $id="", $data) {
        /*return $this->call($type . '/' . $id, array('method' => 'PUT', 'content' => $data));*/
    	$json = json_encode($data);
    	if($id!="")
    		$exec = "curl -XPUT 'http://cdev.nokta.com:9200/".$this->index."/".$type."/".$id."' -d '".$json."'";
    	else 
    		$exec = "curl -XPOST 'http://cdev.nokta.com:9200/".$this->index."/".$type."/' -d '".$json."'";
    	//echo $exec;
    	$out="";
    	$return="";
    	exec($exec,$out,$return);
    }

    //curl -X GET http://localhost:9200/{INDEX}/{TYPE}/_search?q= ...
    function query($type, $q, $start=0, $limit=10, $sort=null) {
        if ($sort){
            $sort = json_encode($sort);
            return $this->call($type . '/_search?' . http_build_query(array('q' => $q, 'from' => $start, 'size' => $limit,"sort" => $sort)));
        }
        else
            return $this->call($type . '/_search?' . http_build_query(array('q' => $q, 'from' => $start, 'size' => $limit)));
    }

}
