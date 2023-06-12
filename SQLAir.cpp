/*
 * Copyright (C) April 16, 2023.

 * A very lightweight (light as air) implementation of a simple CSV-based
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 *
 * Author: Obed Amaning-Yeboah.
 */

#include "SQLAir.h"

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>

#include "HTTPFile.h"

// Setup a server socket to accept connections on the socket.
using namespace boost::asio;
using namespace boost::asio::ip;

// Shortcut to smart pointer with TcpStream.
using TcpStreamPtr = std::shared_ptr<tcp::iostream>;

/**
 * A fixed HTTP response header that is used by the runServer method below.
 * Note that this a constant (and not a global variable).
 */
const std::string HTTPRespHeader =
    "HTTP/1.1 200 OK\r\n"
    "Server: localhost\r\n"
    "Connection: Close\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: ";

/**
 * A helper method to write column titles to the output stream.
 *
 * \param[in] csv The CSV object to select from.
 *
 * \param[in] colNames A vector of column names to select.
 *
 * \param[in] os The output stream to write the results to.
 */
void printColumnTitles(const CSV& csv, const StrVec& colNames,
                       std::ostream& os) {
    std::string delim = "";
    for (const auto& colName : colNames) {
        os << delim << colName;
        delim = "\t";
    }
    os << std::endl;
}

/**
 * A helper method to write the selected row to the output stream.
 *
 * \param[in] csv The CSV object to select from.
 *
 * \param[in] colNames A vector of column names to select.
 *
 * \param[in] row A custom vector-of-string that stores information about each
 * column in a row in a CSV.
 *
 * \param[in] os The output stream to write the results to.
 */
void printSelectedRow(const CSV& csv, const StrVec& colNames, const CSVRow& row,
                      std::ostream& os) {
    std::string delim = "";
    for (const auto& colName : colNames) {
        os << delim << row.at(csv.getColumnIndex(colName));
        delim = "\t";
    }
    os << std::endl;
}

/**
 * A helper method that prints the number of updated rows to the output stream
 * and notifies any waiting threads.
 *
 * \param[in] csv The CSV object to update.
 *
 * \param[in] foundMatch A boolean indicating if a match was found to update.
 *
 * \param[in] numUpdates The number of rows updated.
 *
 * \param[in] os The output stream to write the results to.
 */
void printAndUpdateOrExit(CSV& csv, bool foundMatch, int numUpdates,
                          std::ostream& os) {
    // If a match was found, print the number of updates and notify all waiting
    // threads.
    if (foundMatch) {
        os << numUpdates << " row(s) updated.\n";
        csv.csvCondVar.notify_all();
    } else {
        // If no match was found, and we don't need to wait, print the number of
        // updates and exit.
        os << numUpdates << " row(s) updated.\n";
    }
}

/**
 * A helper method that scans a given CSV object for rows that match a specified
 * condition and updates the selected columns with the specified values.
 *
 * \param[in] csv The CSV object to scan and update.
 *
 * \param[in] colNames A vector of column names to update.
 *
 * \param[in] values A vector of new values to update the selected columns with.
 *
 * \param[in] whereColIdx The index of the column to match the condition
 * against. If set to -1, no column is selected and all rows are updated.
 *
 * \param[in] cond The condition for the WHERE clause.
 *
 * \param[in] value The value to compare the WHERE clause column to.
 *
 * \param[in] numUpdates The number of rows that were updated.
 *
 * \return A boolean indicating if a match was found and rows were updated.
 */
bool SQLAir::scanAndUpdateRows(CSV& csv, StrVec colNames, StrVec values,
                               const int whereColIdx, const std::string& cond,
                               const std::string& value, int& numUpdates) {
    bool foundMatch = false;

    // Scan for rows that match the condition and update them.
    for (auto& row : csv) {
        const bool isMatch = (whereColIdx == -1)
                                 ? true
                                 : matches(row.at(whereColIdx), cond, value);
        if (isMatch) {
            // Update the selected columns for the matching row.
            for (size_t i = 0; i < colNames.size(); ++i) {
                row.at(csv.getColumnIndex(colNames[i])) = values[i];
            }
            numUpdates++;
            foundMatch = true;
        }
    }
    return foundMatch;
}

/**
 * Executes a multithreaded-safe SELECT query on a CSV object and outputs the
 * results to a specified output stream. If colNames contains only "*", all
 * columns will be selected. If whereColIdx is not -1, only rows that match the
 * specified condition will be selected.
 *
 * \param[in] csv The CSV object to select from.
 *
 * \param[in] mustWait Decides if the method will wait until a lock on the CSV
 * object is obtained before continuing.
 *
 * \param[in] colNames A vector of column names to select.
 *
 * \param[in] whereColIdx The index of the column to apply the WHERE clause to.
 * If -1, no WHERE clause will be applied.
 *
 * \param[in] cond The condition for the WHERE clause.
 *
 * \param[in] value The value to compare the WHERE clause column to.
 *
 * \param[in] os The output stream to write the results to.
 */
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names.
    if (colNames.size() == 1 && colNames.front() == "*")
        // With a wildcard column name, we print all of the columns in CSV.
        colNames = csv.getColumnNames();

    // Print column titles only once, when at least one row is selected.
    bool printedColumnTitles = false;
    int numSelects = 0;
    std::unique_lock<std::mutex> lock(csv.csvMutex);
    while (true) {
        for (const auto& row : csv) {
            // Determine if this row matches "where" clause condition, if any.
            if (whereColIdx == -1 || matches(row[whereColIdx], cond, value)) {
                // If this is the first selected row, print the column titles.
                if (!printedColumnTitles) {
                    printColumnTitles(csv, colNames, os);
                    printedColumnTitles = true;
                }

                // Print the selected row.
                printSelectedRow(csv, colNames, row, os);
                numSelects++;
            }
        }
        if (numSelects > 0 || !mustWait) {
            // At least 1 row was selected, or we don't need to wait.
            break;
        }

        // No row was selected, so wait for an update.
        csv.csvCondVar.wait(lock);
    }
    os << numSelects << " row(s) selected.\n";
}

/**
 * Updates rows in a multithreaded-safe manner in a CSV object based on a
 * specified condition and outputs the number of updated rows to a specified
 * output stream. If colNames contains only "*", all columns will be updated.
 *
 * \param[in] csv The CSV object to update.
 *
 * \param[in] mustWait Decides if the method will wait until a lock on the CSV
 * object is obtained before continuing.
 *
 * \param[in] colNames A vector of column names to update.
 *
 * \param[in] values A vector of values to update the specified columns to.
 *
 * \param[in] whereColIdx The index of the column to apply the WHERE clause to.
 * If -1, no WHERE clause will be applied.
 *
 * \param[in] cond The condition for the WHERE clause.
 *
 * \param[in] value The value to compare the WHERE clause column to.
 *
 * \param[in] os The output stream to write the results to.
 */
void SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    // Convert any "*" to suitable column names.
    if (colNames.size() == 1 && colNames.front() == "*")
        // With a wildcard column name, we update all of the columns in CSV.
        colNames = csv.getColumnNames();

    int numUpdates = 0;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(csv.csvMutex);
            bool foundMatch = scanAndUpdateRows(
                csv, colNames, values, whereColIdx, cond, value, numUpdates);

            if (foundMatch || !mustWait) {
                printAndUpdateOrExit(csv, foundMatch, numUpdates, os);
                break;
            }

            // Otherwise, wait for a signal from another thread that the data
            // has changed.
            csv.csvCondVar.wait(lock);
        }
    }
}

/**
 * Inserts a new row into a CSV object in a multithreaded-safe manner and
 * outputs the number of inserted rows to a specified output stream.
 *
 * \param[in] csv The CSV object to insert the new row into.
 *
 * \param[in] mustWait Decides if the method will wait until a lock on the CSV
 * object is obtained before continuing.
 *
 * \param[in] colNames A vector of column names for the new row.
 *
 * \param[in] values A vector of values for the new row.
 *
 * \param[in] os The output stream to write the results to.
 *
 * \throws Exp if the number of column names does not match the number of values
 * provided.
 */
void SQLAir::insertQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, std::ostream& os) {
    if (colNames.size() != values.size()) {
        throw Exp("Number of columns and values don't match.");
    }

    // Create a new row with the specified column names and values.
    CSVRow newRow;
    for (size_t i = 0; i < colNames.size(); ++i) {
        newRow.push_back(values[i]);
    }

    // Add the new row to the CSV object.
    {
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.emplace_back(newRow);
        csv.csvCondVar.notify_all();
        // If the caller wants to wait for confirmation that the new row has
        // been added, wait on the CSV object's condition variable.
        if (mustWait) {
            csv.csvCondVar.wait(lock);
        }
    }
    os << "1 row(s) inserted.\n";
}

/**
 * Deletes rows from a CSV object in a multithreaded-safe manner based on a
 * specified condition and outputs the number of deleted rows to a specified
 * output stream.
 *
 * \param[in] csv The CSV object to delete rows from.
 *
 * \param[in] mustWait Decides if the method will wait until a lock on the CSV
 * object is obtained before continuing.
 *
 * \param[in] whereColIdx The index of the column to apply the WHERE clause to.
 * If -1, no WHERE clause will be applied.
 *
 * \param[in] cond The condition for the WHERE clause.
 *
 * \param[in] value The value to compare the WHERE clause column to.
 *
 * \param[in] os The output stream to write the results to.
 */
void SQLAir::deleteQuery(CSV& csv, bool mustWait, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    int numDeleted = 0;
    {
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        while (true) {
            bool foundMatch = false;
            for (size_t i = 0; i < csv.size(); i++) {
                // Check if this row matches "where" clause condition, if any.
                if (whereColIdx == -1 ||
                    matches(csv[i][whereColIdx], cond, value)) {
                    csv.erase(csv.begin() + i);
                    numDeleted++;
                    i--;
                    foundMatch = true;
                }
            }
            // Notify waiting threads and exit loop if a match is found or
            // mustWait is false.
            if (foundMatch || !mustWait) {
                csv.csvCondVar.notify_all();
                break;
            }
            csv.csvCondVar.wait(lock);
        }
    }
    os << numDeleted << " row(s) deleted.\n";
}

/**
 * Processes HTTP GET requests received via input stream 'is', and responds with
 * an appropriate HTTP response sent to output stream 'os'. The method reads the
 * request path and either processes a SQL Air query or returns a file to the
 * client based on the path. Exceptions generated by the SQLAirBase::process are
 * caught and reported.
 *
 * \param[in] is The input stream containing the HTTP GET request.
 *
 * \param[in] os The output stream to which the HTTP response is sent.
 */
void SQLAir::serveClient(std::istream& is, std::ostream& os) {
    std::string line, path;

    // Read the "GET" word and then the path.
    is >> line >> path;

    // Skip/ignore all the HTTP request & headers for now.
    while (std::getline(is, line) && (line != "\r")) {
    }

    // Check and execute the correct request.
    if (path.find("/sql-air?query=") == 0) {
        // This is a SQL Air query to be processed.
        std::string query = Helper::url_decode(path.substr(15));
        std::ostringstream responseStream;
        try {
            // Process the query and generate the response.
            SQLAirBase::process(query, responseStream);
        } catch (const std::exception& exp) {
            // Catch and report any exceptions generated by SQLAirBase::process.
            responseStream << "Error: " << exp.what() << std::endl;
        }
        std::string response = responseStream.str();

        // Build the HTTP response.
        std::ostringstream oss;
        oss << HTTPRespHeader;
        oss << response.size() << "\r\n";
        oss << "\r\n";
        oss << response;
        os << oss.str();
    } else if (!path.empty()) {
        // In this case we assume the user is asking for a file. Have the helper
        // http class do the processing.
        path = path.substr(1);  // Remove the leading '/' sign.
        // Use the http::file helper method to send the response back to the
        // client.
        os << http::file(path);
    }
}

/**
 * Top-level method to run a custom HTTP server to process sqlair commands.
 *
 * Phase 1: Multithreading is not needed.
 *
 * Phase 2: This method must use multiple threads -- each request
 * should be processed using a separate detached thread. Optional
 * feature: Limit number of detached-threads to be <= maxThreads.
 *
 * \param[in] server The boost::tcp::acceptor object to be used to accept
 * connections from various clients.
 *
 * \param[in] maxThreads The maximum number of threads that the server
 * should use at any given time.
 */
void SQLAir::runServer(boost::asio::ip::tcp::acceptor& server,
                       const int maxThr) {
    // Process client connections one-by-one...forever.
    while (true) {
        // Creates garbage-collected connection on heap.
        TcpStreamPtr client = std::make_shared<tcp::iostream>();
        server.accept(*client->rdbuf());  // Wait for client to connect.

        // Create a new CSV object for each transaction.
        CSV csv;

        // Check if the maximum number of detached threads has been reached.
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        while (numThreads >= maxThr) {
            thrCond.wait(lock);
        }

        // Increment the number of threads.
        numThreads++;

        // Now we have an I/O stream to talk to the client. Have a conversation
        // using the protocol.
        std::thread thr([this, client] {
            serveClient(*client, *client);

            // Decrement the number of threads.
            numThreads--;

            // Notify any waiting threads.
            thrCond.notify_one();
        });

        // Process transaction independently.
        thr.detach();
    }
}

/**
 * Loads CSV data from a URL by sending an HTTP GET request to the specified
 * host and port.
 *
 * \param[in] csv The CSV object to store the loaded data into.
 *
 * \param[in] host The host name of the server to connect to.
 *
 * \param[in] port The port number to connect to on the server.
 *
 * \param[in] path The path to the resource to request from the server.
 *
 * \throws std::runtime_error if there are any errors connecting to or
 * retrieving data from the specified URL.
 */
void SQLAir::loadFromURL(CSV& csv, const std::string& host,
                         const std::string& port, const std::string& path) {
    tcp::iostream client;
    client.connect(host, port);
    if (!client.good()) {
        throw std::runtime_error("Unable to connect to " + host + " at port " +
                                 port);
    }
    client << "GET " << path << " HTTP/1.1\r\n"
           << "Host: " << host << "\r\n"
           << "Connection: Close\r\n\r\n";

    // Get the first HTTP response line and ensure it has 200 OK in it to
    // indicate that the data stream is good.
    std::string statusLine;
    std::getline(client, statusLine);
    if (statusLine.find("200 OK") == std::string::npos) {
        throw std::runtime_error("Error (" + Helper::trim(statusLine) +
                                 ") getting " + path + " from " + host +
                                 " at port " + port);
    }

    // First skip over HTTP response headers.
    for (std::string hdr;
         std::getline(client, hdr) && !hdr.empty() && hdr != "\r";) {
    }

    // The rest of the response contains the CSV data.
    std::stringstream ss;
    ss << client.rdbuf();
    csv.load(ss);
}

/**
 * Loads a CSV file from a specified file path or URL and returns a reference
 * to it. If the CSV has already been loaded, the cached copy is returned
 * instead.
 *
 * \param[in] fileOrURL The path or URL of the CSV file to load.
 *
 * \return A reference to the loaded CSV object.
 *
 * \throws std::exception If an error occurs while loading the CSV file.
 */
CSV& SQLAir::loadAndGet(std::string fileOrURL) {
    // Check if the specified fileOrURL is already loaded in a thread-safe
    // manner to avoid race conditions on the unordered_map.
    {
        std::scoped_lock<std::mutex> guard(recentCSVMutex);

        // Use recent CSV if parameter was an empty string.
        fileOrURL = (fileOrURL.empty() ? recentCSV : fileOrURL);

        // Update the most recently used CSV for the next round.
        recentCSV = fileOrURL;
        if (inMemoryCSV.find(fileOrURL) != inMemoryCSV.end()) {
            // Requested CSV is already in memory. Just return it.
            return inMemoryCSV.at(fileOrURL);
        }
    }

    // When control drops here, we need to load the CSV into memory. Loading of
    // I/O is being done outside of critical sections.
    CSV csv;  // Load data into this csv.
    if (fileOrURL.find("http://") == 0) {
        // This is a URL, so we have to get the stream from a web-server.
        std::string host, port, path;
        std::tie(host, port, path) = Helper::breakDownURL(fileOrURL);
        loadFromURL(csv, host, port, path);
    } else {
        // We assume it is a local file on the server. Load that file.
        std::ifstream data(fileOrURL);

        // This method may throw exceptions on errors.
        csv.load(data);
    }

    // We get to this line of code only if the above if-else to load the
    // CSV did not throw any exceptions. In this case we have a valid CSV
    // to add to our inMemoryCSV list. We need to do that in a thread-safe
    // manner.
    std::scoped_lock<std::mutex> guard(recentCSVMutex);

    // Move (instead of copy) the CSV data into our in-memory CSVs.
    inMemoryCSV[fileOrURL].move(csv);

    // Return a reference to the in-memory CSV (not temporary one).
    return inMemoryCSV.at(fileOrURL);
}

// Save the currently loaded CSV file to a local file.
void SQLAir::saveQuery(std::ostream& os) {
    if (recentCSV.empty() || recentCSV.find("http://") == 0) {
        throw Exp("Saving CSV to an URL using POST is not implemented");
    }

    // Create a local file and have the CSV write itself.
    std::ofstream csvData(recentCSV);
    inMemoryCSV.at(recentCSV).save(csvData);
    os << recentCSV << " saved.\n";
}
