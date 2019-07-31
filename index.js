const https = require('https');
const fs = require('fs');
const mysql = require('mysql');
const yargs = require('yargs');

const UTF8_BOM = '\u{FEFF}';

const argv = yargs
    .option('move', {
        alias: 'm',
        description: 'Move the data files into a test folder rather than deleting them',
        type: 'boolean',
    })
    .help()
    .alias('help', 'h')
    .argv;


let idx = 0, 
	pidx = 0, 
	items = null,
	temp_totalProcessed = 0;

let _config = {
	apiKey: 'TX3IJ7Q5D2MJBC4S',
	stockSymbols: ['INTC', 'AMD'],
	lastDateFetched: 0
};

const sql = "INSERT INTO stock_prices (symbol,timestamp,open,close,high,low,volume,interval_minutes) VALUES ?";

var _getRawData = (stockSymbol, callback) => {
	https.get('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + stockSymbol + '&interval=1min&outputsize=full&apikey=' + _config.apiKey, (resp) => {
	  let data = '';

	  // A chunk of data has been received.
	  resp.on('data', (chunk) => {
			data += chunk;
	  });

	  // The whole response has been received. Print out the result.
	  resp.on('end', () => {

		  console.log('Data Received');

		  callback(data);

		  //console.log(JSON.parse(data).explanation);
	  });

	}).on('error', (err) => {
		console.log('Error: ' + err.message);
		callback(false);
	});
};

var _logToFile = (stockSymbol, rawData, callback) => {

	console.log('Logging ' + _config.stockSymbols[idx]);

	let _date = (new Date()).getDate(); // 1 - 31
	let _year = (new Date()).getFullYear(); // 2019
	let _month = (new Date()).getMonth() + 1; // 0 = January

	fs.writeFile('./data/' + stockSymbol + ' ' + (_year + '-' + _month + '-' + _date) + '.json', rawData, (err) => {
		if(err) {
			console.log(err);
			return callback(false);
		}
		console.log('The file was saved!');
		callback(true);
	});

};

var _sleepLongTerm = () => {
	let _day = (new Date()).getDay(); // Sunday - Saturday : 0 - 6
	let _hour = (new Date()).getHours(); // between 0 and 23
	let _year = (new Date()).getFullYear(); // 2019
	let _month = (new Date()).getMonth(); // 0 = January

	// Mon - Fri only
	if(_day >= 1 && _day <= 5) {

		let currentDate = (new Date());
		currentDate.setHours(0,0,0,0);
		console.log('Current Date Check: ', currentDate.getTime());
		console.log('Last Fetched Date Check: ', _config.lastDateFetched);

		// See if the date changed to the next day
		if(_config.lastDateFetched !== currentDate.getTime()) {

			// Is it after 5pm
			if(_hour >= 17) {

				idx = 0;
				_looper(); // Start fetching stocks again

				return true; // Do not sleep any more
			}
		}
	}

	// Check every minute if it's 5:00pm Mon-Friday EST
	setTimeout(() => {
		// Read any changes that might have been made
		_readConfigFile(false, () => {
			_sleepLongTerm(); // Check again
		});
	}, 60000);
};

var _setLastFetched = () => {
	let currentDate = (new Date());
	currentDate.setHours(0,0,0,0);
	console.log('Current Date: ', currentDate);

	_config.lastDateFetched = currentDate.getTime();
};

var _sleepNextStock = () => {
	setTimeout(() => {

		idx++;

		_looper(); // Get the next stock data

	}, 15000);
};

var _looper = () => {

	console.log('Fetching ' + _config.stockSymbols[idx]);

	_getRawData(_config.stockSymbols[idx], (rawData) => {

		if(rawData !== false) {
			// TODO: Reduce payload size before logging to file
			_logToFile(_config.stockSymbols[idx], rawData, (result) => {

				if(idx < _config.stockSymbols.length-1) {
					_sleepNextStock();
				} else {

					_setLastFetched();

					_writeConfigFile((saveResult) => {
						_processDataFiles();
						_sleepLongTerm();
					});
				}
			});
		} else {
			if(idx < _config.stockSymbols.length-1) {
				_sleepNextStock();
			} else {
				_setLastFetched();

				_writeConfigFile((saveResult) => {
					_processDataFiles();
					_sleepLongTerm();
				});
			}
		}
	});
};

var _readDataFile = (filename, callback) => {
	fs.readFile('./data/' + filename, 'utf8', (err, data) => {
		if(!err) {
			
			if (data.startsWith(UTF8_BOM)) {
				data = data.substring(UTF8_BOM.length); 
			}
			
			callback(data);
			return;
		}
		
		console.log(err);
		callback(false);
	});
};

var _nextDataFile = () => {
	pidx++;

	if(pidx < items.length) {
		_processDataFile();
	}
	else {
		console.log(`Finished processing files ${temp_totalProcessed}/${items.length}`);
		
		temp_totalProcessed = 0;
		items = null;
	}
};

var _moveDataFileToErrorFolder = (filename, callback) => {
	fs.rename('./data/' + filename, './data/errors/' + filename, function (err) {
		if (err) console.log(err);
		console.log('Successfully moved: ' + filename);
		callback(!err);
	})
};

var _moveDataFileToTestFolder = (filename, callback) => {
	fs.rename('./data/' + filename, './data/test/' + filename, function (err) {
		if (err) console.log(err);
		console.log('Successfully moved: ' + filename);
		callback(!err);
	})
};


var _deleteDataFile = (filename, callback) => {
	fs.unlink('./data/' + filename, (err) => {
	  if (err)
		console.error(err)

	  callback(!err);
	})
};

var _processDataFile = () => {

	_readDataFile(items[pidx], (result) => {
		if(result !== false) {
			var connection = mysql.createConnection({
			  host     : 'localhost',
			  user     : 'nodejs',
			  password : 'Nodejs101+1',
			  database : 'fintech'
			});
			
			let _sData = JSON.parse(result);
			let _values = [];
			
			if(_sData && _sData['Time Series (1min)'] && _sData['Meta Data'])
			{
				let _symbol = _sData['Meta Data']['2. Symbol'];
				
				let _seeker = null;
				
				let _processed = 0, _total = 0;
				let _maxDate = 0;
				
				for(let k in _sData['Time Series (1min)']) {
					let _cData = Date.parse(k);
					
					if(_cData > _maxDate)
						_maxDate = _cData;
				}
				
				if(_maxDate > 0) {
					
					let _date = (new Date(_maxDate)).getDate(); // 1 - 31
					let _year = (new Date(_maxDate)).getFullYear(); // 2019
					let _month = (new Date(_maxDate)).getMonth() + 1; // 0 = January 0 - 11
					_seeker = `${_year}-${_month < 10 ? '0' + _month : _month}-${_date}`; // yyyy-mm-dd
					
					for(let k in _sData['Time Series (1min)']) {
						
						let _timeSeries = _sData['Time Series (1min)'][k];
						
						if(k.indexOf(_seeker) > -1) {
							_values.push([
								_symbol,
								k,
								parseFloat(_timeSeries['1. open']),
								parseFloat(_timeSeries['4. close']),
								parseFloat(_timeSeries['2. high']),
								parseFloat(_timeSeries['3. low']),
								parseFloat(_timeSeries['5. volume']),
								1
							]);
							
							_processed++;
						}
						
						_total++;
					}
				}
				
				console.log(`Processed: ${_processed} / ${_total}`)
				
				if(_processed > 0) {
					connection.query(sql, [_values], function(err) {
						connection.end();
						
						if(err) {
							console.log(err);
							_moveDataFileToErrorFolder(items[pidx],(r) => {_nextDataFile();});
						} else {
							if (argv.move)
								_moveDataFileToTestFolder(items[pidx],(r) => {_nextDataFile();});
							else
								_deleteDataFile(items[pidx], (r) => {_nextDataFile();});
							temp_totalProcessed++;
						}
						console.log('Processed file successfully ' + items[pidx]);
					});
				} else {
					console.log('No data found - ' + items[pidx]);
					_moveDataFileToErrorFolder(items[pidx],(r) => {_nextDataFile();});
				}
			} else {
				console.log('Invalid JSON - ' + items[pidx]);
				_moveDataFileToErrorFolder(items[pidx],(r) => {_nextDataFile();});
				
			}
		} else {
			console.log('Failed to read file data - ' + items[pidx]);
			_moveDataFileToErrorFolder(items[pidx],(r) => {_nextDataFile();});
			
		}
	});
};

var _processDataFiles = () => {
	
	items = null;	
	pidx = 0;
	
	fs.readdir('./data/', { withFileTypes: true }, function(err, _items) {		
		if(_items !== null && _items.length > 0) {
			
			items = _items
			.filter(dirent => !dirent.isDirectory())
			.map(dirent => dirent.name);
			
			_processDataFile();
		} else {
			console.log('No items no found');
		}
	});	
};

var _readConfigFile = (log, callback) => {
	fs.readFile('config.json', 'utf8', (err, data) => {
		if(!err) {
			
			if (data.startsWith(UTF8_BOM)) {
				data = data.substring(UTF8_BOM.length); 
			}
			
			log && console.log(data);
			
			_config = JSON.parse(data);
		}
		else console.log(err);
		
		callback();
	});
};

var _writeConfigFile = (callback) => {
	fs.writeFile('config.json', JSON.stringify(_config), (err) => {
		if(err) {
			console.log(err);
			return callback(false);
		}
		console.log('Config file was saved!');
		callback(true);
	});
};

 _readConfigFile(true, () => {

	if (!fs.existsSync('./data')){
		fs.mkdirSync('./data');
	}
	
	if (!fs.existsSync('./data/errors')){
		fs.mkdirSync('./data/errors');
	}
	
	if (!fs.existsSync('./data/test')){
		fs.mkdirSync('./data/test');
	}
	
	_processDataFiles();
	_sleepLongTerm();
});

