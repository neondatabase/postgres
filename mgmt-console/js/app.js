import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom';
import Loader from "react-loader-spinner";
import { Router, Route, Link, IndexRoute, hashHistory, browserHistory } from 'react-router';

function ServerStatus(props) {
    const datadir = props.server.datadir;
    const status = props.server.status;
    const port = props.server.port;

    return (
	<div>
	    <h2>{ datadir == 'primary' ? 'Primary' : datadir }</h2>
	    status: <div className='status'>{status}</div><br/>
	    to connect: <span className='shellcommand'>psql -h { window.location.hostname } -p { port } -U zenith postgres</span><br/>
	</div>
    );
}

function StandbyList(props) {
    const bucketSummary = props.bucketSummary;
    const standbys = props.standbys;
    const maxwalpos = bucketSummary.maxwal ? walpos_to_int(bucketSummary.maxwal) : 0;

    const [walposInput, setWalposInput] = useState({ src: 'text', value: '0/0'});

    // find earliest base image
    const minwalpos = bucketSummary.nonrelimages ? bucketSummary.nonrelimages.reduce((minpos, imgpos_str, index, array) => {
	const imgpos = walpos_to_int(imgpos_str);
	return (minpos == 0 || imgpos < minpos) ? imgpos : minpos;
    }, 0) : 0;

    const can_create_standby = minwalpos > 0 && maxwalpos > 0 && maxwalpos >= minwalpos;
    var walpos_valid = true;

    function create_standby() {
	const formdata = new FormData();
	formdata.append("walpos", walposStr);

	props.startOperation('Creating new standby at ' + walposStr + '...',
			     fetch("/create_standby", { method: 'POST', body: formdata }));
    }

    function destroy_standby(datadir) {
	const formdata = new FormData();
	formdata.append("datadir", datadir);
	props.startOperation('Destroying ' + datadir + '...',
			     fetch("/destroy_server", { method: 'POST', body: formdata }));
    }

    const handleSliderChange = (event) => {
	setWalposInput({ src: 'slider', value: event.target.value });
    }    

    const handleWalposChange = (event) => {
	setWalposInput({ src: 'text', value: event.target.value });
    }

    var sliderValue;
    var walposStr;
    if (walposInput.src == 'text')
    {
	const walpos = walpos_to_int(walposInput.value);

	if (walpos >= minwalpos && walpos <= maxwalpos)
	    walpos_valid = true;
	else
	    walpos_valid = false;
	
	sliderValue = Math.round((walpos - minwalpos) / (maxwalpos - minwalpos) * 100);
	walposStr = walposInput.value;
    }
    else
    {
	const slider = walposInput.value;
	const new_walpos = minwalpos + slider / 100 * (maxwalpos - minwalpos);

	console.log('minwalpos: '+ minwalpos);
	console.log('maxwalpos: '+ maxwalpos);

	walposStr = int_to_walpos(Math.round(new_walpos));
	walpos_valid = true;
	console.log(walposStr);
    }

    var standbystatus = ''
    if (standbys)
    {
	standbystatus = 
	    <div>
		<h2>Standbys</h2>
		{
		    standbys.length > 0 ? 
 			standbys.map((server) =>
			    <>
				<ServerStatus key={ 'status_' + server.datadir} server={server}/>
				<button key={ 'destroy_' + server.datadir} onClick={e => destroy_standby(server.datadir)}>Destroy standby</button>
			    </>
			) : "no standby servers"
		}
	    </div>
    }

    return (
	<div>
	    { standbystatus }
	    <br/>
	    <button onClick={create_standby} disabled={!can_create_standby || !walpos_valid}>Create new Standby</button> at LSN 
            <input type="text" id="walpos_input" value={ walposStr } onChange={handleWalposChange} disabled={!can_create_standby}/>
	    <input type="range" id="walpos_slider" min="0" max="100" steps="1" value={sliderValue}  onChange={handleSliderChange} disabled={!can_create_standby}/>
	    <br/>
	</div>
    );
}

function ServerList(props) {
    const primary = props.serverStatus ? props.serverStatus.primary : null;
    const standbys = props.serverStatus ? props.serverStatus.standbys : [];
    const bucketSummary = props.bucketSummary;

    var primarystatus = '';

    function destroy_primary() {
	const formdata = new FormData();
	formdata.append("datadir", 'primary');
	props.startOperation('Destroying primary...',
			     fetch("/destroy_server", { method: 'POST', body: formdata }));
    }    

    function restore_primary() {
	props.startOperation('Restoring primary...',
			     fetch("/restore_primary", { method: 'POST' }));
    }    
    
    if (primary)
    {
	primarystatus =
	    <div>
		<ServerStatus server={primary}/>
		<button onClick={destroy_primary}>Destroy primary</button>
	    </div>
    }
    else
    {
	primarystatus =
	    <div>
		no primary server<br/>
		<button onClick={restore_primary}>Restore primary</button>
	    </div>
    }

    return (
	<div>
	    <h1>Server status</h1>
	    { primarystatus }
	    <StandbyList standbys={standbys} startOperation={props.startOperation} bucketSummary={props.bucketSummary}/>
	</div>
    );
}

function BucketSummary(props) {
    const bucketSummary = props.bucketSummary;
    const startOperation = props.startOperation;

    function slicedice() {
	startOperation('Slicing sequential WAL to per-relation WAL...',
		       fetch("/slicedice", { method: 'POST' }));
    }
    
    if (!bucketSummary.nonrelimages)
    {
	return <div>
		   Storage Bucket Status loading...
	       </div>
    }

    return (
	<div>
	    <h1>Storage bucket status</h1>
	    <div>Base images at following WAL positions:
		<ul>
		    {bucketSummary.nonrelimages.map((img) => (
			<li key={img}>{img}</li>
		    ))}
		</ul>
	    </div>
            Sliced WAL is available up to { bucketSummary.maxwal }<br/>
	    Raw WAL is available up to { bucketSummary.maxseqwal }<br/>

	    <br/>
	    <button onClick={slicedice}>Slice & Dice WAL</button>
	</div>
    );
}

function ProgressIndicator()
{
    return (
	<div>
	    <Loader
		type="Puff"
		color="#00BFFF"
		height={100}
		width={100}
	    />
	</div>
    )
}

function walpos_to_int(walpos)
{
    const [hi, lo] = walpos.split('/');

    return parseInt(hi, 16) + parseInt(lo, 16);
}

function int_to_walpos(x)
{
    console.log('converting ' + x);
    return (Math.floor((x / 0x100000000)).toString(16) + '/' + (x % 0x100000000).toString(16)).toUpperCase();
}

function OperationStatus(props) {
    const lastOperation = props.lastOperation;
    const inProgress = props.inProgress;
    const operationResult = props.operationResult;

    if (lastOperation)
    {
	return (
	    <div><h2>Last operation:</h2>
		<div>{lastOperation} { (!inProgress && lastOperation) ? 'done!' : '' }</div>
		<div className='result'>
		    {inProgress ? <ProgressIndicator/> : <pre>{operationResult}</pre>}
		</div>
	    </div>
	);
    }
    else
	return '';
}

function ActionButtons(props) {

    const startOperation = props.startOperation;
    const bucketSummary = props.bucketSummary;
    
    function reset_demo() {
	startOperation('resetting everything...',
		       fetch("/reset_demo", { method: 'POST' }));
    }

    function init_primary() {
	startOperation('Initializing new primary...',
		       fetch("/init_primary", { method: 'POST' }));
    }

    function zenith_push() {
	startOperation('Pushing new base image...',
		       fetch("/zenith_push", { method: 'POST' }));
    }
	
    return (
	<div>
	    <button onClick={reset_demo}>RESET DEMO</button>

	    <button onClick={init_primary}>Init primary</button>

	    <button onClick={zenith_push}>Push base image</button>

	</div>
    );
}

function Sidenav(props)
{
    const toPage = (page) => (event) => {
	event.preventDefault()
	props.switchPage(page);
    };
    return (
	<div>
	    <h3 className="sidenav-item">Menu</h3>
	    <a href="/servers" onClick={toPage('servers')} className="sidenav-item">Servers</a>
	    <a href="/storage" onClick={toPage('storage')} className="sidenav-item">Storage</a>
	    <a href="/demo" onClick={toPage('demo')} className="sidenav-item">Demo</a>
	    <a href="#import" className="sidenav-item">Import / Export</a>
	    <a href="#jobs" className="sidenav-item">Jobs</a>
	</div>
    );
}

function App()
{
    const [page, setPage] = useState('servers');
    const [serverStatus, setServerStatus] = useState({});
    const [bucketSummary, setBucketSummary] = useState({});
    const [lastOperation, setLastOperation] = useState('');
    const [inProgress, setInProgress] = useState('');
    const [operationResult, setOperationResult] = useState('');

    useEffect(() => {
	reloadStatus();
    }, []);

    function startOperation(operation, promise)
    {
	promise.then(result => result.text()).then(resultText => {
	    operationFinished(resultText);
	});
	
	setLastOperation(operation);
	setInProgress(true);
	setOperationResult('');
    }

    function operationFinished(result)
    {
	setInProgress(false);
	setOperationResult(result);
	reloadStatus();
    }

    function clearOperation()
    {
	setLastOperation('')
	setInProgress('');
	setOperationResult('');
	console.log("cleared");
    }
    
    function reloadStatus()
    {
	fetch('/server_status').then(res => res.json()).then(data => {
	    setServerStatus(data);
	});

	fetch('/bucket_summary').then(res => res.json()).then(data => {
	    setBucketSummary(data);
	});
    }

    const content = () => {
	console.log(page);
	if (page === 'servers') {
	    return (
		    <ServerList startOperation={ startOperation }
				serverStatus={ serverStatus }
				bucketSummary={ bucketSummary }/>
		);
	} else if (page === 'storage') {
	    return (
		<BucketSummary startOperation={ startOperation }
			       bucketSummary={ bucketSummary }/>
	    );
	} else if (page === 'demo') {
	    return (
		<ActionButtons startOperation={ startOperation }
			       bucketSummary={ bucketSummary }/>
	    );
	}
    }

    function switchPage(page)
    {
	console.log("topage " + page);
	setPage(page)
	clearOperation();
    };

    return (
	<div className="row">
	    <div className="sidenav">
		<Sidenav switchPage={switchPage} className="column"/>
	    </div>
	    <div className="column">
		<div>
		    { content() }
		</div>
		<OperationStatus lastOperation={ lastOperation }
				 inProgress = { inProgress }
				 operationResult = { operationResult }/>
	    </div>
	</div>
    );
}

ReactDOM.render(<App/>, document.getElementById('reactApp'));
