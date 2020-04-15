/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

extern crate getopts;

use std::env;
use std::fs;
use std::convert::TryInto;

use getopts::Options;

use serde::Deserialize;
use serde_json::Deserializer;
use statemap::Statemap;

use chrono::{DateTime, Utc, NaiveDateTime};

/*
 * TraceData represents the default non-verbose MinIO trace format. If the
 * MinIO trace format changes in the future this will also need to be updated.
 */
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct TraceData {
    host: String,
    time: DateTime<Utc>,
    client: String,
    call_stats: CallStats,
    api: String,
    path: String,
    query: String,
    status_code: u32,
    status_msg: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct CallStats {
    rx: u32,
    tx: u32,
    duration: u64,
    time_to_first_byte: u32,
}

/*
 * Parse the MinIO trace data file and print statemap-formatted records to
 * stdout.
 */
fn print_states(filename: &str, title: &str, cluster: &str)
    -> std::io::Result<()> {

    let raw_data = fs::read_to_string(filename)?;

    let mut sm = Statemap::new(title, Some(cluster.to_string()), None);

    let state_iter = Deserializer::from_str(&raw_data)
        .into_iter::<TraceData>();

    for deserialize_result in state_iter {
        let td = deserialize_result.expect("invalid minio json");

        /*
         * MinIO's trace data is sorted by _end_ time of operation, not _start_
         * time. Further, MinIO doesn't report the start time of each operation.
         * MinIO only reports the end time of each operation and the duration of
         * the operation, so we must infer the start time based on this
         * information.
         */
        let end_ns: u64 = td.time.timestamp_subsec_nanos().into();
        let unix_end_time: u64 = (td.time.timestamp() * 1_000_000_000)
            .try_into()
            .expect("failed to make unix timestamp into ns timestamp");

        let end_time_ns = unix_end_time + end_ns;
        let begin_time_ns = end_time_ns - td.call_stats.duration;

        let begin_s: i64 = (begin_time_ns / 1_000_000_000) as i64;
        let begin_ns: u32 = (begin_time_ns % 1_000_000_000) as u32;

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(
            begin_s, begin_ns), Utc);

        /*
         * Set this minio instance to be working on the given API request.
         * Immediately after it is done serving an API request we switch it
         * to the 'waiting' state.
         */
        sm.set_state(&td.host, &td.api, None, dt);
        sm.set_state(&td.host, "waiting", None, td.time);
    }
    sm.set_state_color("waiting", "white");

    for state in sm {
        println!("{}", state);
    }

    Ok(())

}

fn usage(opts: Options, msg: &str) {
    let synopsis = "\
        Convert MinIO JSON trace output to statemap input";

    let usg = format!("minio-statemap - {}", synopsis);
    let ex_usg = "Example usage:\n \
        ./minio-statemap -i ./my_minio_trace.out > minio_states\n"
        .to_string();
    println!("{}", opts.usage(&usg));
    println!("{}", ex_usg);
    println!("{}", msg);
}

fn main() -> std::io::Result<()> {

    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.reqopt("i",
                "input-file",
                "path to minio trace file to be parsed",
                "FILE");

    opts.optopt("c",
                "cluster-name",
                "name of the cluster for display in the rendered statemap",
                "NAME");
    opts.optopt("t",
                "title",
                "statemap title",
                "TITLE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            usage(opts, &f.to_string());
            return Ok(())
        },
    };

    let ifile = matches.opt_str("input-file").unwrap();
    let cluster = matches.opt_get_default(
        "cluster-name", "minio cluster".to_string()).unwrap();
    let title = matches.opt_get_default(
        "title", "MinIO".to_string()).unwrap();

    print_states(&ifile, &title, &cluster)
}
