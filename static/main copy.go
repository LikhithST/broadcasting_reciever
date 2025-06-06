package mainn

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const statsInterval = time.Second * 1

// we create a new custom metric of type counter
var webrtcStats = struct {
	PacketsReceived     *prometheus.GaugeVec
	PacketsLost         *prometheus.GaugeVec
	Jitter              *prometheus.GaugeVec
	BytesReceived       *prometheus.GaugeVec
	HeaderBytesReceived *prometheus.GaugeVec
	FIRCount            *prometheus.GaugeVec
	PLICount            *prometheus.GaugeVec
	NACKCount           *prometheus.GaugeVec
}{
	PacketsReceived: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_packets_received_total",
			Help: "Total number of packets received in WebRTC stream",
		},
		[]string{"packets_received"}, // Labels: user and stream_id to track specific streams
	),
	PacketsLost: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_packets_lost_total",
			Help: "Total number of packets lost in WebRTC stream",
		},
		[]string{"packets_lost"},
	),
	Jitter: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_jitter",
			Help: "Current jitter (in ms) in WebRTC stream",
		},
		[]string{"jitter"},
	),
	BytesReceived: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_bytes_received_total",
			Help: "Total bytes received in WebRTC stream",
		},
		[]string{"bytes_received"},
	),
	HeaderBytesReceived: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_header_bytes_received_total",
			Help: "Total header bytes received in WebRTC stream",
		},
		[]string{"header_bytes_received"},
	),
	FIRCount: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_fir_count_total",
			Help: "Total number of FIR (Full Intra Request) packets in WebRTC stream",
		},
		[]string{"fir_count"},
	),
	PLICount: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_pli_count_total",
			Help: "Total number of PLI (Picture Loss Indication) packets in WebRTC stream",
		},
		[]string{"pli_count"},
	),
	NACKCount: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_nack_count_total",
			Help: "Total number of NACK (Negative Acknowledgement) packets in WebRTC stream",
		},
		[]string{"nack_count"},
	),
}

func init() {
	httpSDPServer(8081)
	// we need to register the counter so prometheus can collect this metric
	log.Println("init() function called")
	prometheus.MustRegister(
		webrtcStats.PacketsReceived,
		webrtcStats.PacketsLost,
		webrtcStats.Jitter,
		webrtcStats.BytesReceived,
		webrtcStats.HeaderBytesReceived,
		webrtcStats.FIRCount,
		webrtcStats.PLICount,
		webrtcStats.NACKCount,
	)
}

func main() {
	// Everything below is the Pion WebRTC API! Thanks for using it ❤️.

	// Create a MediaEngine object to configure the supported codec
	mediaEngine := &webrtc.MediaEngine{}

	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		panic(err)
	}

	// Create a InterceptorRegistry. This is the user configurable RTP/RTCP Pipeline.
	// This provides NACKs, RTCP Reports and other features. If you use `webrtc.NewPeerConnection`
	// this is enabled by default. If you are manually managing You MUST create a InterceptorRegistry
	// for each PeerConnection.
	interceptorRegistry := &interceptor.Registry{}

	statsInterceptorFactory, err := stats.NewInterceptor()
	if err != nil {
		panic(err)
	}

	var statsGetter stats.Getter
	statsInterceptorFactory.OnNewPeerConnection(func(_ string, g stats.Getter) {
		statsGetter = g
	})
	interceptorRegistry.Add(statsInterceptorFactory)

	// Use the default set of Interceptors
	if err = webrtc.RegisterDefaultInterceptors(mediaEngine, interceptorRegistry); err != nil {
		panic(err)
	}

	// Create the API object with the MediaEngine
	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithInterceptorRegistry(interceptorRegistry))

	// Prepare the configuration
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}

	// Create a new RTCPeerConnection
	peerConnection, err := api.NewPeerConnection(config)
	if err != nil {
		panic(err)
	}

	// Allow us to receive 1 audio track, and 1 video track
	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		panic(err)
	} else if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo); err != nil {
		panic(err)
	}

	// Set a handler for when a new remote track starts. We read the incoming packets, but then
	// immediately discard them
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) { //nolint: revive
		fmt.Printf("New incoming track with codec: %s\n", track.Codec().MimeType)

		go func() {
			// Print the stats for this individual track
			for {
				stats := statsGetter.Get(uint32(track.SSRC()))

				fmt.Printf("Stats for: %s\n", track.Codec().MimeType)
				fmt.Println(stats.InboundRTPStreamStats)

				webrtcStats.PacketsReceived.WithLabelValues("PacketsReceived").Add(float64(stats.InboundRTPStreamStats.PacketsReceived))
				webrtcStats.PacketsLost.WithLabelValues("PacketsLost").Add(float64(stats.InboundRTPStreamStats.PacketsLost))
				webrtcStats.Jitter.WithLabelValues("Jitter").Set(stats.InboundRTPStreamStats.Jitter)
				webrtcStats.BytesReceived.WithLabelValues("BytesReceived").Add(float64(stats.InboundRTPStreamStats.BytesReceived))
				webrtcStats.HeaderBytesReceived.WithLabelValues("HeaderBytesReceived").Add(float64(stats.InboundRTPStreamStats.HeaderBytesReceived))
				webrtcStats.FIRCount.WithLabelValues("FIRCount").Add(float64(stats.InboundRTPStreamStats.FIRCount))
				webrtcStats.PLICount.WithLabelValues("PLICount").Add(float64(stats.InboundRTPStreamStats.PLICount))
				webrtcStats.NACKCount.WithLabelValues("NACKCount").Add(float64(stats.InboundRTPStreamStats.NACKCount))

				time.Sleep(statsInterval)
			}
		}()

		rtpBuff := make([]byte, 1500)
		for {
			_, _, readErr := track.Read(rtpBuff)
			if readErr != nil {
				panic(readErr)
			}
		}
	})

	var iceConnectionState atomic.Value
	iceConnectionState.Store(webrtc.ICEConnectionStateNew)

	// Set the handler for ICE connection state
	// This will notify you when the peer has connected/disconnected
	peerConnection.OnICEConnectionStateChange(func(connectionState webrtc.ICEConnectionState) {
		fmt.Printf("Connection State has changed %s \n", connectionState.String())
		iceConnectionState.Store(connectionState)
	})

	// Wait for the offer to be pasted
	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		panic(err)
	}

	// Sets the LocalDescription, and starts our UDP listeners
	err = peerConnection.SetLocalDescription(offer)
	if err != nil {
		panic(err)
	}

	// fmt.Println("local desc", peerConnection.LocalDescription())

	// Convert LocalDescription to JSON
	// offerJSON, err := json.Marshal(peerConnection.LocalDescription())
	// if err != nil {
	// 	panic(err)
	// }
	localdescription := encode(peerConnection.LocalDescription())

	resp, err := http.Post("http://localhost:8080/offer", "text/plain", bytes.NewBuffer([]byte(localdescription)))
	if err != nil {
		panic(err)
	}
	// defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println("Response from server:", string(body))

	answer := webrtc.SessionDescription{}
	decode(string(body), &answer)

	// Set the remote SessionDescription
	err = peerConnection.SetRemoteDescription(answer)
	if err != nil {
		panic(err)
	}

	// Create answer
	// answer, err := peerConnection.CreateAnswer(nil)
	// if err != nil {
	// 	panic(err)
	// }

	// Create channel that is blocked until ICE Gathering is complete
	// gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	// Sets the LocalDescription, and starts our UDP listeners
	// err = peerConnection.SetLocalDescription(answer)
	// if err != nil {
	// 	panic(err)
	// }

	// Block until ICE Gathering is complete, disabling trickle ICE
	// we do this because we only can exchange one signaling message
	// in a production application you should exchange ICE Candidates via OnICECandidate
	// <-gatherComplete

	// Output the answer in base64 so we can paste it in browser
	// fmt.Println(encode(peerConnection.LocalDescription()))

	for {
		time.Sleep(statsInterval)

		// Stats are only printed after completed to make Copy/Pasting easier
		if iceConnectionState.Load() == webrtc.ICEConnectionStateChecking {
			continue
		}

		// Only print the remote IPs seen
		for _, s := range peerConnection.GetStats() {
			switch stat := s.(type) {
			case webrtc.ICECandidateStats:
				if stat.Type == webrtc.StatsTypeRemoteCandidate {
					fmt.Printf("%s IP(%s) Port(%d)\n", stat.Type, stat.IP, stat.Port)
				}
			default:
			}
		}
	}
}

// httpSDPServer starts a HTTP Server that consumes SDPs.
func httpSDPServer(port int) {
	http.Handle("/metrics", promhttp.Handler())
	// http.HandleFunc("/offer", func(res http.ResponseWriter, req *http.Request) {
	// 	res.Header().Set("Access-Control-Allow-Origin", "*")
	// 	res.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	// 	res.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// 	// Handle preflight request
	// 	if req.Method == "OPTIONS" {
	// 		res.WriteHeader(http.StatusOK)
	// 		return
	// 	}

	// 	if req.Method != http.MethodPost {
	// 		http.Error(res, "Invalid request method", http.StatusMethodNotAllowed)
	// 		return
	// 	}
	// 	body, _ := io.ReadAll(req.Body)
	// 	sdpChan <- string(body)
	// 	// recieve from channel A
	// 	response_string := <-ch
	// 	// fmt.Fprintf(res, response_string) //nolint: errcheck
	// 	// fmt.Printf("%+v", response_string)
	// 	res.Header().Set("Content-Type", "text/plain")
	// 	res.Write([]byte(response_string))

	// })

	go func() {
		// nolint: gosec
		panic(http.ListenAndServe(":"+strconv.Itoa(port), nil))
	}()

}

// JSON encode + base64 a SessionDescription.
func encode(obj *webrtc.SessionDescription) string {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}

	return base64.StdEncoding.EncodeToString(b)
}

// Decode a base64 and unmarshal JSON into a SessionDescription.
func decode(in string, obj *webrtc.SessionDescription) {
	b, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		panic(err)
	}

	if err = json.Unmarshal(b, obj); err != nil {
		panic(err)
	}
}
