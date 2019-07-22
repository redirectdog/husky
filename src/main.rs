#[derive(Debug)]
enum Error {
    FailedToFetchTask(postgres::Error),
    NoTasks,
    FailedToSaveResult(postgres::Error),
}

fn main() {
    let db = postgres::Connection::connect(
        std::env::var("DATABASE_URL").expect("Missing DATABASE_URL"),
        postgres::TlsMode::None,
    )
    .expect("Failed to connect to database");

    let expected_addresses: Vec<std::net::IpAddr> = std::env::var("EXPECTED_ADDRESSES")
        .expect("Missing EXPECTED_ADDRESSES")
        .split(',')
        .map(|src| src.parse().expect("Failed to parse address"))
        .collect();

    let task_stmt = db.prepare("SELECT id, host FROM redirects WHERE record_confirmed=FALSE AND (last_dns_check IS NULL OR (localtimestamp - last_dns_check) > '1 MINUTE') ORDER BY last_dns_check NULLS FIRST LIMIT 1").expect("Failed to prepare statement");

    let report_stmt = db
        .prepare(
            "UPDATE redirects SET record_confirmed=$2, last_dns_check=localtimestamp WHERE id=$1",
        )
        .expect("Failed to prepare statement");

    let resolver = trust_dns_resolver::Resolver::default().expect("Failed to create resolver");

    loop {
        let result = task_stmt
            .query(&[])
            .map_err(Error::FailedToFetchTask)
            .and_then(|rows| {
                if rows.is_empty() {
                    Err(Error::NoTasks)
                } else {
                    let row = rows.get(0);
                    let id: i32 = row.get(0);
                    let host: String = row.get(1);

                    println!("Got task: {}", host);

                    Ok((id, host))
                }
            })
            .and_then(|(id, host)| {
                let res = resolver.lookup_ip(&host);
                let confirmed = match res {
                    Ok(addrs) => {
                        let mut confirmed = false;
                        for found_addr in addrs {
                            for expected_addr in &expected_addresses {
                                if expected_addr == &found_addr {
                                    confirmed = true;
                                    break;
                                }
                            }

                            if confirmed {
                                break;
                            }
                        }

                        confirmed
                    }
                    Err(err) => {
                        println!("Failed to resolve: {:?}", err);
                        false
                    }
                };
                report_stmt
                    .execute(&[&id, &confirmed])
                    .map_err(Error::FailedToSaveResult)
            });

        if let Err(err) = result {
            if let Error::NoTasks = err {
                std::thread::sleep(std::time::Duration::new(30, 0));
            } else {
                eprintln!("Error: {:?}", err);
                std::thread::sleep(std::time::Duration::new(5, 0));
            }
        }
    }
}
