/*
signifier entry point.

AGPL3.
*/
#[macro_use]
extern crate log;
extern crate flame;
//extern crate collections;
extern crate env_logger;
extern crate regex;
extern crate rusted_cypher;
/**
librarian builds an inverted index of all files/words and writes it to
standard out, suitable for grepping.
*/

mod queue;
mod barrier;
use queue::Queue;
use barrier::Barrier;
use regex::Regex;
use std::thread;
use std::fs::File;
use std::error::Error;
use std::io::prelude::*;
use std::io;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::metadata;
use std::iter::Iterator;
use std::time::Duration;
use rusted_cypher::{GraphClient,Statement};
use rusted_cypher::cypher::CypherQuery;

/// Opens a file, checks to see if it's really a file, then reads the
/// first 4K bytes, then returns false if any are not 7-bit ascii or
/// the file isn't a file.
fn is_7_bit_ascii(filename : String ) -> bool {

    match metadata(filename.clone()) {
        Ok(md) => {
            if md.is_dir() {
                debug!("Turns out file {} is a directory", filename);
                return false;
            }
        }
        Err(problem) => {
            error!("file {} unable to get metadata", filename);
            error!("problem {:?}", problem);
            return false;
        }
    }

    //Assume 7 bits...
    let mut is_7_bit = true;

    let f = match File::open(filename.clone()) {
        Ok(result) => result,
        Err(_) => {
            error!("Unable to open file {}", filename);
            return false;
        }
    };

    debug!("Analyzing file {}", filename);

    //pull off the first block of the file and test to see if it
    // conforms to 7-bit ascii.
    for byte in f.bytes().take(4096) {
        match byte {
            Ok(byte) => {
                // 7-bit, LF, CR...
                if (byte < 32 || byte > 126) && byte != 10 && byte != 12 {
                    // Whoops! Assumption wrong!
                    debug!("Byte {} not 7-bit", byte);
                    is_7_bit = false;
                    break;
                }
            }

            Err(_) => { break }
        }
    }

    debug!("File {} is {}, apparently", filename,
           match is_7_bit { true => "ascii", false => "binary"} );

    return is_7_bit;

}

/**
Reads the data in `filename` and returns a `string`, utf8-lossy..
*/
fn slurp_string(filename : String) -> String {

    let mut filehandle = match File::open(filename.clone()) {
        Ok(result) => result,
        Err(_) => panic!("Unable to open file {}", filename)
    };

    let mut filedata : Vec<u8> = Vec::new();

    match filehandle.read_to_end(&mut filedata) {
        Ok(_ ) => {}
        Err(problem) => panic!(problem)
    };

    let result : String = String::from_utf8_lossy(&filedata).into_owned();

    return result;

}

fn use_file(filename : &str) -> bool {
    let final_char = filename
        .chars()
        .last();

    return filename != "" &&
        ! (filename.contains(".git") || filename.contains(".hg")) &&
        match final_char { Some(c) => c != '~', None => false }
}

fn dispatch_neo4j(query : CypherQuery, comment : &str) {
    debug!("Sending query to neo4j");
    match query.send() {
        Ok(_) => {},
        Err(e) => {
            error!("error sending query for {} - {} - {} - {}",
                   comment,
                   e,
                   e.description(),
                   match e.cause() {
                       Some(e) => format!("{}", e),
                       None => "unk cause".to_string()})
        }
    }
}

fn main() {
    env_logger::init().unwrap();
    debug!("logging at debug");
    info!("logging at info");


    // use the stdin for the input
    let mut filenames : Vec<String> = Vec::new();

    flame::start("reading stdin");
    // Read standard input.
    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(n) => {
                let filename : String = input.trim().to_string();
                if use_file(&filename) {
                    debug!("{}", filename);
                    filenames.push(filename);
                }
                if n == 0 {
                    break;
                }
            }
            Err(_) => {break;}
        }
    }
    flame::end("reading stdin");

    let (mut tx_filenames, rx_filenames) : (Queue<String>, Queue<String>) = queue::new();

    // this must be the same as tx_filenames
    let mut files_to_read_barrier = Barrier::new(filenames.len());

    debug!("Processing {} filenames", filenames.len());

    {



        // Text reading loop.
        for filename in filenames {

            tx_filenames.push(filename.clone());
        }



    }

    let (tx_collector, mut rx_collector) :
    (Queue<(String, BTreeSet<String>)>,
     Queue<(String, BTreeSet<String>)>) = queue::new();
    let word_classification = Regex::new(r"(\w{3,}|\d{3,})").unwrap();

    flame::start("reading files and regexing");
    for _ in 0..3 {
        let mut tx_collector = tx_collector.clone();
        let mut rx_filenames = rx_filenames.clone();
        let mut files_to_read_barrier = files_to_read_barrier.clone();
        let word_classification = word_classification.clone();

        thread::spawn(move || {
            let graph = match GraphClient::connect(
                "http://neo4j:password@localhost:7474/db/data") {
                Ok(g) => { g }
                Err(e) => { panic!(e)}
            };

            loop {
                match rx_filenames.pop() {
                    Some(filename) => {
                        if is_7_bit_ascii(filename.clone()) {

                            //unable to slurp string if not 7-bit
                            // ascii. regrettably, this results in the
                            // first 4K being double read.
                            let text : String = slurp_string(filename.clone());
                            {
                                let mut query = graph.query();
                                match Statement::new(
                                    "MERGE (f:filename { name: {filename}})")
                                    .with_param("filename", &filename) {
                                        Ok(x) => {query.add_statement(x);}
                                        Err(e) => {error!("{}",e)}
                                    };

                                dispatch_neo4j(query, "Loading filenames");
                            }

                            debug!("Chomping on {}", filename);

                            let chunk_max = 200;
                            {
                                let mut query = graph.query();

                                let mut chunker = 0;

                                let words = word_classification.captures_iter(&text);

                                let mut set: BTreeSet<String> = BTreeSet::new();
                                for cap in words {
                                    let s = cap.at(1).unwrap_or("").to_lowercase();
                                    set.insert(s.clone());

                                    let mut stmt = Statement::new(
                                        "MERGE (w:word { name: {word}})");
                                    // TODO: cleanse
                                    for (k,v) in vec![("word", &s)] {
                                        match stmt.add_param(k,v) {
                                            Ok(_) => {}
                                            Err(e) => {error!("{} - {}",e, e.description())}
                                        }
                                    }
                                    query.add_statement(stmt);

                                    if chunker > chunk_max {
                                        dispatch_neo4j(query, "Chunked insert words");
                                        query = graph.query();
                                        chunker = 0;
                                    }
                                    chunker += 1;
                                }

                                dispatch_neo4j(query, "insert of words");
                            }
                            {
                                let mut query = graph.query();
                                let mut chunker = 0;

                                let words = word_classification.captures_iter(&text);
                                for cap in words {
                                    let word = cap.at(1).unwrap_or("").to_lowercase();
                                    let mut stmt = Statement::new(
                                        "MATCH (f:filename { name: {filename}}), (w:word { name: {word}})
MERGE (f)-[:CONTAINS]->(w)");
                                    for (k,v) in vec![("word", &word), ("filename", &filename)] {
                                        match stmt.add_param(k,v) {
                                            Ok(_) => {}
                                            Err(e) => {error!("{} - {}",e, e.description())}
                                        }
                                    }
                                    query.add_statement(stmt);

                                    if chunker > chunk_max {
                                        dispatch_neo4j(query, "Chunked insert relationships");
                                        query = graph.query();
                                        chunker = 0;
                                    }
                                    chunker += 1;

                                }

                                dispatch_neo4j(query, "insert of relationships");
                                //                            tx_collector.push((filename.clone(), set));
                            }
                        }
                        debug!("incrementing barrier, wrapped up {}", &filename.clone());
                        // increment the barrier regardless of 7-bit-ascii-ness
                        files_to_read_barrier.reach();


                    }
                    None => {
                        info!("Popped a filename from the incoming queue (est length {}) and didn't get anything...", rx_filenames.len());
                        // delay slightly
                        thread::sleep(Duration::from_millis(50));
                    }

                }


                // Did we get every file?
                if files_to_read_barrier.reached_p() {
                    info!("Done with work, thread out!");
                    break;
                }
            }
        });
    }
    debug!("Waiting on everyone to finish processing...");
    files_to_read_barrier.wait_for_everyone();
    flame::end("reading files and regexing");


    debug!("Preparing to print... approximately {} files indexed", rx_collector.clone().len());

    flame::start("building inverted index");
    // Pick off the valid data.
    let mut word_file_map : HashMap<String, Vec<String>> = HashMap::new();
    let graph = match GraphClient::connect(
        "http://neo4j:password@localhost:7474/db/data") {
        Ok(g) => { g }
        Err(e) => { panic!(e)}
    };
    loop {
        match rx_collector.pop() {
            Some((filename, set)) => {
                let mut query = graph.query();
                let mut chunker = 0;
                let chunk_max = 200;

                for word in set {
                    /*
                    let entry =  word_file_map
                    .entry(word)
                    .or_insert(Vec::new());
                    entry.push(filename.clone());
                     */

                    let mut stmt = Statement::new(
                        "MATCH (f:filename { name: {filename}}), (w:word { name: {word}})
MERGE (f)-[:CONTAINS]->(w)");
                    for (k,v) in vec![("word", &word), ("filename", &filename)] {
                        match stmt.add_param(k,v) {
                            Ok(_) => {}
                            Err(e) => {error!("{} - {}",e, e.description())}
                        }
                    }
                    query.add_statement(stmt);

                    if chunker > chunk_max {
                        dispatch_neo4j(query, "Chunked insert relationships");
                        query = graph.query();
                        chunker = 0;
                    }
                    chunker += 1;

                }

                dispatch_neo4j(query, "insert of relationships");
            }
            // at this point everything that is here should be here.
        None => { break; }
        };
    }
/*
    flame::end("building inverted index");

    flame::start("writing information out");
    // ad hoc WAG of memory
//    let mut string = String::with_capacity(filename_count  * 32);
    for (word, filenames) in word_file_map {
        let record = word + ": " + filenames.join(", ").as_str();
        println!("{}", record.as_str());
    }
    flame::end("writing information out");
*/
    flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
}
