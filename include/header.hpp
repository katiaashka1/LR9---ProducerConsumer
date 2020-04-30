// Copyright 2018 Your Name <your_email>

#ifndef INCLUDE_HEADER_HPP_
#define INCLUDE_HEADER_HPP_

#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/bind/bind.hpp>
#include <boost/thread/mutex.hpp>
#include <utility>
#include <gumbo.h>
#include <queue>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <cstdlib>
#include <string>
#include <fstream>
#include "sertificate.hpp"


namespace beast = boost::beast; // from <boost/beast.hpp>
namespace http = beast::http;   // from <boost/beast/http.hpp>
namespace net = boost::asio;    // from <boost/asio.hpp>
namespace ssl = net::ssl;       // from <boost/asio/ssl.hpp>
using tcp = net::ip::tcp;       // from <boost/asio/ip/tcp.hpp>



struct HrefData {
    std::string link = "";
    uint64_t rang = 0;
};


class Crawler {
public:
    explicit Crawler(std::string beginPage = "https://yandex.ru",
                     uint64_t maxDepth = 1,
                     uint8_t producerThreadsCount = 10,
                     uint8_t consumerThreadsCount = 10,
                     std::string output = "allLinks.txt") :
            startingPoint(std::move(beginPage)), depth(maxDepth),
            networkThreadsCount(producerThreadsCount),
            parserThreadsCount(consumerThreadsCount),
            outputPath(std::move(output)) {
    }

    void handler() {
        HrefData fatherOfAll{startingPoint, 0};
        hrefQueue.push(fatherOfAll);
        imgQueue.push(startingPoint);
        boost::thread_group hrefFabric;
        boost::thread_group imgFabric;
        for (uint8_t i = 0; i < networkThreadsCount; ++i)
            hrefFabric.create_thread(boost::bind(&Crawler::
            hrefWorker, this, i));

        hrefFabric.join_all();
        for (uint8_t i = 0; i < parserThreadsCount; ++i)
            imgFabric.create_thread(boost::bind(&Crawler::imgWorker, this));
        imgFabric.join_all();
    }

    void hrefWorker(uint16_t id) {
        while (true) {
            try {
                hrefMuter.lock();
                if (!hrefQueue.empty()) {
                    HrefData href = hrefQueue.front();
                    hrefQueue.pop();
                    hrefMuter.unlock();
                    if (href.link.empty()) continue;

                    std::string page = getPage(href.link);

                    if (page.empty())continue;
                    getLinks(fromStrToNode(page)->root, href);
                    hrefMuter.lock();
                    std::cout << id << ": " << href.link <<
                              " - " << href.rang << std::endl;
                    hrefMuter.unlock();
                } else {
                    hrefMuter.unlock();
                    break;
                }
            }
            catch (...) {
                hrefMuter.unlock();
                continue;
            }
        }
    }

    void imgWorker() {
        while (true) {
            try {
                imgMuter.lock();
                if (!imgQueue.empty()) {
                    std::string img = imgQueue.front();
                    imgQueue.pop();
                    imgMuter.unlock();
                    if (img.empty()) continue;
                    std::string page = getPage(img);
                    if (page.empty())continue;
                    getImg(fromStrToNode(page)->root);
                } else {
                    imgMuter.unlock();
                    break;
                }
            }
            catch (...) {
                imgMuter.unlock();
                continue;
            }
        }
    }

    static GumboOutput *fromStrToNode(std::string &str) {
        GumboOutput *output = gumbo_parse(str.c_str());
        return output;
    }

    void getLinks(GumboNode *node, const HrefData &parent) {
        try {
            if (node->type != GUMBO_NODE_ELEMENT) {
                return;
            }
            GumboAttribute *href;
            if (node->v.element.tag == GUMBO_TAG_A &&
                (href = gumbo_get_attribute(&node->v.element.attributes,
                                            "href"))) {
                std::string s = href->value;
                if (s != "#" && s != parent.link && s.find("http") == 0) {
                    if (parent.rang < depth) {
                        HrefData childHref{href->value, parent.rang + 1};

                        hrefMuter.lock();
                        hrefQueue.push(childHref);
                        hrefMuter.unlock();

                        imgMuter.lock();
                        imgQueue.push(href->value);
                        imgMuter.unlock();
                    }
                }
            }
            GumboVector *children = &node->v.element.children;
            for (unsigned int i = 0; i < children->length; ++i) {
                getLinks(static_cast<GumboNode *>(children->data[i]), parent);
            }
        }
        catch (...) { return; }
    }

    void getImg(GumboNode *node) {
        try {
            if (node->type != GUMBO_NODE_ELEMENT) {
                return;
            }
            GumboAttribute *img;
            if ((node->v.element.tag == GUMBO_TAG_IMG ||
                 node->v.element.tag == GUMBO_TAG_IMAGE) &&
                (img = gumbo_get_attribute(&node->v.element.attributes,
                                           "src"))) {
                std::string s = img->value;
                if (s.find("http") == 0) {
                    imgMuter.lock();
                    std::ofstream out(outputPath, std::ios::app);
                    if (out.is_open()) {
                        // std::cout<<"IMAGE: "<<s<<std::endl;
                        out << "IMAGE: " << s << std::endl;
                    }
                    out.close();
                    imgMuter.unlock();
                }
            }
            GumboVector *children = &node->v.element.children;
            for (unsigned int i = 0; i < children->length; ++i) {
                getImg(static_cast<GumboNode *>(children->data[i]));
            }
        }
        catch (...) { return; }
    }

    static std::string getPage(std::string url) {
        std::string page;
        if (getPort(url) == "80") {
            page = getHttp(url);
        } else { page = getHttps(url); }
        return page;
    }

    static std::string getHttp(std::string url) {
        try {
            std::string const host = getHost(url);
            std::string const port = "80"; // https - 443, http - 80
            std::string const target = getTarget(url);
            int version = 10;
            boost::asio::io_context ioc;
            tcp::resolver resolver{ioc};
            boost::beast::tcp_stream stream{ioc};
            auto const results = resolver.resolve(host, port);
            stream.connect(results);
            http::request<http::string_body> req{http::verb::get, target,
                                                 version};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
            http::write(stream, req);
            boost::beast::flat_buffer buffer;
            http::response<http::dynamic_body> res;
            http::read(stream, buffer, res);
            return boost::beast::buffers_to_string(res.body().data());
        }
        catch (...) {
            return "";
        }
    }

    static std::string getHttps(std::string &url) {
        std::string const host = getHost(url);
        std::string const port = "443"; // https - 443, http - 80
        std::string const target = getTarget(url);
        int version = 11; // или 10 для http 1.0
        try {
            boost::asio::io_context ioc;
            ssl::context ctx{ssl::context::sslv23_client};
            load_root_certificates(ctx);
            tcp::resolver resolver{ioc};
            ssl::stream<tcp::socket> stream{ioc, ctx};
            if (!SSL_set_tlsext_host_name(stream.native_handle(),
                                          host.c_str())) {
                boost::system::error_code ec{static_cast<int>(
                              ::ERR_get_error()),
                               boost::asio::error::get_ssl_category()};
                throw boost::system::system_error{ec};
            }
            auto const results = resolver.resolve(host, port);
            boost::asio::connect(stream.next_layer(), results.begin(),
                                 results.end());
            stream.handshake(ssl::stream_base::client);
            http::request<http::string_body> req{http::verb::get,
                                                 target, version};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
            http::write(stream, req);
            boost::beast::flat_buffer buffer;
            http::response<http::dynamic_body> res;
            http::read(stream, buffer, res);
            return boost::beast::buffers_to_string(res.body().data());
        } catch (...) {
            return "";
        }
    }

    static std::string getHost(std::string &url) {
        std::string host;
        int64_t skipHTTP = 0;
        int64_t skipHTTPS = 0;
        int64_t pos = 0;
        skipHTTPS = url.find("https");
        skipHTTP = url.find("http");
        if (skipHTTPS != -1)pos += skipHTTPS + 3 + 5;
        else if (skipHTTP != -1)pos += skipHTTP + 3 + 4;
        int64_t endOfHost = url.find('/', pos);
        if (endOfHost == -1)endOfHost = url.size();
        for (int64_t i = pos; i < endOfHost; ++i)
            host.push_back(url[i]);
        return host;
    }

    static std::string getTarget(std::string &url) {
        std::string target;
        int64_t www = url.find("www");
        int64_t skipWWW = 0;
        if (www != -1) skipWWW = www + 2;
        int64_t endOfHost = url.find('.', skipWWW);
        int64_t targetStartPos = url.find('/', endOfHost);
        for (uint64_t i = targetStartPos; i < url.size(); ++i) {
            target.push_back(url[i]);
        }
        if (target[target.size() - 1] != '/') target.push_back('/');
        return target;
    }

    static std::string getPort(std::string &url) {
        int64_t portFlag = url.find("https");
        if (portFlag != -1) return "443";
        return "80";
    }

public:
    std::string startingPoint;
    uint64_t depth;
    uint8_t networkThreadsCount;
    uint8_t parserThreadsCount;
    std::string outputPath;

private:
    std::queue<HrefData> hrefQueue;
    std::queue<std::string> imgQueue;
    boost::mutex hrefMuter;
    boost::mutex imgMuter;
};

#endif // INCLUDE_HEADER_HPP_
