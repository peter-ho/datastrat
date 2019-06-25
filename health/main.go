package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

        "net"
        "sync"
        "encoding/json"
        "time"
)

const version = "1.0.0"

type HelloResponse struct {
        Message string `json:"message"`
}
type VersionResponse struct {
        Version string `json:"version"`
}

func HelloHandler(w http.ResponseWriter, r *http.Request) {
        response := HelloResponse{
                Message: "Hello",
        }
        json.NewEncoder(w).Encode(response)
        return
}

func VersionHandler(w http.ResponseWriter, r *http.Request) {
        response := VersionResponse{
                Version: version,
        }
        json.NewEncoder(w).Encode(response)
        return
}

func LoggingHandler(h http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                fmt.Printf("%s - - [%s] \"%s %s %s\" \n", time.Now().Format(time.RFC1123),
                       r.Method, r.URL.Path, r.Proto, r.UserAgent())
                h.ServeHTTP(w, r)
        })
}


var (
        healthStatus = http.StatusOK
        readyStatus = http.StatusOK
        mu sync.RWMutex
)

func HealthStatus() int {
        mu.RLock()
        defer mu.RUnlock()
        return healthStatus
}

func SetHealthStatus(status int) {
        mu.Lock()
        healthStatus = status
        mu.Unlock()
}

func ReadyStatus() int {
        mu.RLock()
        defer mu.RUnlock()
        return readyStatus
}

func SetReadyStatus(status int) {
        mu.Lock()
        readyStatus = status
        mu.Unlock()
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(HealthStatus())
}

func ReadyHandler(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(ReadyStatus())
}

func ReadyStatusHandler(w http.ResponseWriter, r *http.Request) {
        switch ReadyStatus() {
        case http.StatusOK:
               SetReadyStatus(http.StatusServiceUnavailable)
        case http.StatusServiceUnavailable:
               SetReadyStatus(http.StatusOK)
        }
        w.WriteHeader(http.StatusOK)
}

func HealthStatusHandler(w http.ResponseWriter, r *http.Request) {
        switch HealthStatus() {
        case http.StatusOK:
               SetHealthStatus(http.StatusServiceUnavailable)
        case http.StatusServiceUnavailable:
               SetHealthStatus(http.StatusOK)
        }
        w.WriteHeader(http.StatusOK)
}

func Serve(s *http.Server) error {
        addr := s.Addr
        if addr == "" {
                addr = ":http"
        }
        listener, err := net.Listen("tcp", addr)
        if err != nil {
                return err
        }
        return s.Serve(listener)
}

func main() {
	var (
		httpAddr   = flag.String("http", "0.0.0.0:80", "HTTP service address.")
		healthAddr = flag.String("health", "0.0.0.0:81", "Health service address.")
	)
	flag.Parse()

	log.Println("Starting server...")
	log.Printf("Health service listening on %s", *healthAddr)
	log.Printf("HTTP service listening on %s", *httpAddr)

	errChan := make(chan error, 10)

	hmux := http.NewServeMux()
	hmux.HandleFunc("/health", HealthHandler)
	hmux.HandleFunc("/health/status", HealthStatusHandler)
	hmux.HandleFunc("/ready", ReadyHandler)
	hmux.HandleFunc("/ready/status", ReadyStatusHandler)
	healthServer := new(http.Server)
	healthServer.Addr = *healthAddr
	healthServer.Handler = LoggingHandler(hmux)

	go func() {
		errChan <- Serve(healthServer)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", HelloHandler)
	//mux.Handle("/secure", handlers.JWTAuthHandler(handlers.HelloHandler))
	mux.HandleFunc("/version", VersionHandler)

	httpServer := new(http.Server)
	httpServer.Addr = *httpAddr
	httpServer.Handler = LoggingHandler(mux)

	go func() {
		errChan <- Serve(httpServer)
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case err := <-errChan:
			if err != nil {
				log.Fatal(err)
			}
		case s := <-signalChan:
			log.Println(fmt.Sprintf("Captured %v. Exiting...", s))
			SetReadyStatus(http.StatusServiceUnavailable)
                        //result := s.shutdown
                        //<-s.shutdownFinished
                        //log.Println(fmt.Sprintf("Shutdown %v .", result))
			os.Exit(0)
		}
	}
}
