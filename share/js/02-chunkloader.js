try {
  ChunkLoader == undefined;
}
catch(e){
class ChunkLoader extends ScriptLoader {
	constructor(selfSrc){
		super(selfSrc);

    let parts=this.src.split("/");
    //parts.pop();
    parts.pop();
    parts.pop();
    parts.pop();
    
    
    this.buildRoot=parts.join("/")+"/";
    //List of objects or callbacks to call as data is being loaded

		this.manifest=[];
    this.path_manifest={};

		this.chunksExpected=0;
		this.chunksLoaded=0;

		this.tags={};
		this.urlQueue=[];
		this.urlMap={};
		this.availableQueue=[];
		this.working=0;
		this.workingLimit=4;
		this.resolver;
		let scope=this;
		this.ready=new Promise((resolve,reject)=>{
			scope.resolver=resolve;
		});
		return this;
	}	

  updateStatus(message,mode){
    //console.log("UPDATE STATUS CALLED");
      let e=new CustomEvent("JPACK_STATUS",{detail: {progress: 100*this.chunksLoaded/this.chunksExpected, message:message, mode:mode}});
      window.dispatchEvent(e);

  }

	/**
	 * Load the pako.js into worker pool for delfate support
   * This is actaully prencoded (no compression) for dynamic loading
	 */
	bootstrap(){

    // Path is relative to this.buildRoot
    return this.queueChunkScript("data/jpack/bootstrap.jpack")
      .then((data)=>{
        let decoder=new TextDecoder("utf-8");
        let string=decoder.decode(data);
        return this.pool.addScriptBody(string)
        .then(()=>{
          // Unload scripts
          this.unloadScript(this.buildRoot+"data/jpack/bootstrap.jpack");

          this.chunksLoaded++;
          this.updateStatus("Boot Complete");

          return Promise.resolve();
        });

    });
	}




	//This callback is executed from a chunk script to decode/decompress the data
	//
	decodeData(options, dataFunc){
		//console.log("decodeData script call");
    let src=options.jpack_path;
    options.jpack_path=options.jpack_path.substr(this.buildRoot.length);
		let e=this.urlMap[options.jpack_path];
		e.options=options;
		e.dataFunc=dataFunc;
		e.resolver(e);
		this._executeNext();
	}

	/*Send data to worker pool for decodeing
	 */
	_decodeChunk(e){
		switch(e.options.jpack_type){
			case "data":
      case "boot":
				//console.log("SENDING DATA");
        //console.log(e.dataFunc());
				return this.pool.queueFunction("decode",{options:e.options,string:e.dataFunc()},[])
				.then((res)=>{
					//res.result is the decoded chunk data to now send to channel manager
					return Promise.resolve(res.result);
				})
				break;
			default:
				break;
		}

	}
	/**
   * Queues a request to download a chunk script. As chunks are large and the
   * order is important, a limited number of chunks are downloaded at one time
	 *
   * Returns a promise when the chunkscript has been downloaded, prased and
   * decoded.  The promise resolves to an arraybuffer of the decoded data
   *
   * PATHS ARE RELATIVE TO BUILD ROOT ie this.buildRoot
   * This is to match the build output paths of the jpack files
	 */

	queueChunkScript(path){
		//Add this to the queue 
		let promise;
		let entry={path:path, dataFunc:undefined};
		entry.promise=new Promise((resolve,reject)=>{
			entry.resolver=resolve;
			entry.rejecter=reject;
		});
		this.urlQueue.push(entry);
		this.urlMap[path]=entry;
		this._executeNext();

		//This promise is resolved when the script is ready to be decoded
		//ie when the dataFunc field has been assigned
		return entry.promise
		.then((e)=>{
			//Queue the decoding into the worker pool
			//console.log("ABOUT TO _decodeChunk");
			return this._decodeChunk(e);
		});
	}

	_executeNext(){
		if(this.urlQueue.length>0){
			let e=this.urlQueue.shift()
			this._loadChunk(e); //This gives a promise but the actual loadded script calls the decode directly
		}
                /****************************************************************************************/
                /* console.log("Working limit: ", this.workingLimit,"currently working", this.working); */
                /* if((this.working<this.workingLimit) && (this.urlQueue.length>0)){                    */
                /*         console.log("EXECUTE NEXT");                                                 */
                /*         this.working++;                                                              */
                /*         let e=this.urlQueue.shift()                                                  */
                /*         this._loadChunk(e);                                                          */
                /* }                                                                                    */
                /****************************************************************************************/
	}


	/* loads the chunk. The returned promise is not used.
	 * The load is complete when the chunk calls the decodeData callback
	 */
	_loadChunk(entry){
		//Run this when a decoder is available	
    let path=this.buildRoot+entry.path;
    //console.log("_loadChunk", path);
		return new Promise((resolve,reject)=>{
			setTimeout(()=>{
				this.loadScript(path).then((e)=>{
					this.updateStatus("Loading data "+ entry.path);
					//e.parentElement.removeChild(e);
					resolve();
				})
				.catch((e)=>{
					this.updateStatus("Error data "+ entry.path);
					console.log("Caught error");
					entry.rejecter("Could not load script");
				});
			},0);
		});
	}

  //Fails/ends when two items can not be loaded
	load_rec(path, callback){
		let head=path;
		let segPath;;	
		let p= Promise.resolve();
		let resolver;
		let rejecter;

		let last=new Promise((resolve, reject)=>{
			resolver=resolve;
			rejecter=reject;
		});

    let limit=1000;
		let scope=this;

    let fail_count=0;
    let stack=[];

		function next(seq){
			//setTimeout(()=>{
        let prefix= stack.map((e)=>{return sprintf("%032X", e)}).join("/");
        if(prefix.length){
          prefix+="/";
        }
				segPath=sprintf("%s/%s%032X.jpack",head,prefix,seq);
        
				p=p.then(()=>{
					return chunkLoader.queueChunkScript(segPath);
				})
					.then((data)=>{
						scope.updateStatus("Building channels from "+segPath);

            // Delete the script element  as we no longer need it
						return callback(data);//channelManager.buildChannels(data);
					})
					.then(()=>{
            scope.chunksLoaded++;
						scope.updateStatus("Building channels complete "+segPath);

            scope.unloadScript(scope.buildRoot+segPath);

						seq++;	
						if(seq<limit){
							next(seq);
              fail_count=0;   //Reset fail count
							return Promise.resolve();
						}
						else {
							resolver();
						}
					})

					.catch((e)=>{
						console.log("CATCH",e);
            fail_count++;
            console.log("fail count", fail_count);
            switch(fail_count){
              case 1:
                // Reached end of current dir push child
                stack.push(1);
                //i=1; 
                next(1);
                break;

              case 2:
                // Could not read child dir, try sibling
                stack.pop();
                let v=stack.pop();
                v++;
                stack.push(v);
                //i=1;
                next(1);
                break;

              case 3:
                // Could not read sibling dir
						    rejecter();
                break
              default:
                //  Should not get here
						    rejecter();
                break;
            }
            
						  //rejecter();
					})

					.finally(()=>{
            // Unload scripts
            scope.unloadScript(scope.buildRoot+segPath);
						return Promise.resolve();
					})


			//},0);
		}
		next(1);

		return last.then(()=>{
		}).catch((e)=>{

    })
    .finally(()=>{
			  this.updateStatus("Loading Complete", 1);
        //console.log("LOAD COMPLETE");
			return Promise.resolve();

    });
	}

  //Load the application
  app(){
    this.load_rec("app/jpack",  (data)=>{
      // Expected the content is javascript. Create a script element, with the content and append to head?
      let s=document.createElement("script");
      s.innerHTML=data;
    });
  }

	/**
	 * Load a sequential set of chunk files. The files are sequencially numbered'
	 */
	load(path, callback){
		let head=path;
		let segPath;;	
		let p= Promise.resolve();
		let resolver;
		let rejecter;
		let last=new Promise((resolve, reject)=>{
			resolver=resolve;
			rejecter=reject;
		});
    let limit=1000;
		//limit=limit==undefined?1000:limit;
		let i=1;	
		let scope=this;

		function next(seq){
			setTimeout(()=>{
				segPath=sprintf("%s/%032X.jpack",head,i);;	
				p=p.then(()=>{
					return chunkLoader.queueChunkScript(segPath);
				})
					.then((data)=>{
						scope.updateStatus("Building channels from "+segPath);

            // Delete the script element  as we no longer need it
						return callback(data);//channelManager.buildChannels(data);
					})
					.then(()=>{
            scope.chunksLoaded++;
						scope.updateStatus("Building channels complete "+segPath);


						i++;	
						if(i<limit){
							next(i);
							return Promise.resolve();
						}
						else {
							resolver();
						}
					})
					.catch((e)=>{
						console.log("CATCH",e);
						rejecter();
					})
					.finally(()=>{
            // Unload scripts
            scope.unloadScript(scope.buildRoot+segPath);
						return Promise.resolve();
					})


			},0);
		}
		next(i);

		return last.then(()=>{
		}).catch((e)=>{

    })
    .finally(()=>{
			  this.updateStatus("Loading Complete", 1);
        //console.log("LOAD COMPLETE");
			return Promise.resolve();

    });
	}

  // This should only be called once during load
  set_path_manifest(hash){
    this.path_manifest=hash;
  }

	//Called by JSONP manifest files
	addToManifest(list){
		console.log("ADDING to manifest ", list);
		this.manifest=this.manifest.concat(list);
	}




	setTarget(div){
		//console.log("Set target");
		//this.target=div;
		//this.display=new LoadingDisplay(div);
	}

	//In live mode this appends the datagram 
	appendDatagram(d){
		//lookup the topic code in tags
	}
}

var chunkLoader=new ChunkLoader(document.currentScript.src);	//GLOBAL chunkLoader

window.ChunkLoader=ChunkLoader;

//Setup the status display and bootstrap with pako data
window.addEventListener("load", (e)=>{
  chunkLoader.booted=true;
	chunkLoader.bootstrap()
        .then(()=>{
            console.log("Bootstrap finished");
            //chunkLoader.setStatusDisplay(new LoadingDisplay(document.body));
		        return Promise.resolve();
        })
        .then(()=>{
                console.log("About to call resolver");
                chunkLoader.resolver();
                //Load the app here
        });
});


function chunkLoaded(event){
	//console.log(event);
	//Monitor the progress here
	console.log("PROGRESS");
}

console.log("HELLO");
//channelManager=new ChannelManager(scriptLoader);
}
