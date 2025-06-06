// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js
// +build !js

// broadcast demonstrates how to broadcast a video to many peers, while only requiring the broadcaster to upload once.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	fastclock "github.com/likhith/fastclock"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/intervalpli"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Define a struct type for data channel messages
type DataChannelMessage struct {
	FrameID                int64  `json:"frameID"`
	MessageSentTimeClient2 int64  `json:"messageSentTime_client2,omitempty"`
	MessageSentTimeSfu2    int64  `json:"messageSentTime_sfu2,omitempty"`
	MessageSentTimeSfu1    int64  `json:"messageSentTime_sfu1,omitempty"`
	MessageSentTimeClient1 int64  `json:"messageSentTime_client1,omitempty"`
	JitterSFU2             int64  `json:"jitter_sfu2,omitempty"`
	JitterSFU1             int64  `json:"jitter_sfu1,omitempty"`
	LatencyEndToEnd        int64  `json:"latency_end_to_end,omitempty"`
	MessageSendRate        int64  `json:"message_send_rate,omitempty"`
	Payload                []byte `json:"payload"`
}

// Example variable declaration (if needed)
// var dataChannelMessage DataChannelMessage

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
	FrameID             *prometheus.GaugeVec
	MessageSendRate     *prometheus.GaugeVec
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
	MessageSendRate: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webrtc_message_send_rate",
			Help: "Rate of messages sent in WebRTC stream",
		},
		[]string{"message_send_rate"},
	),
}

func init() {
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
		webrtcStats.MessageSendRate,
	)
}

// nolint:gocognit, cyclop
func main() {
	port_s1 := flag.Int("port_s1", 8082, "http server port")
	port_s2 := flag.Int("port_s2", 8083, "http server port")
	flag.Parse()

	ch := make(chan string)

	sdpChan := httpSDPServer(*port_s1, ch)

	httpStaticServer(*port_s2)

	// Everything below is the Pion WebRTC API, thanks for using it ❤️.
	// offer := webrtc.SessionDescription{}
	// decode(<-sdpChan, &offer)
	fmt.Println("")

	// peerConnectionConfig := webrtc.Configuration{
	// 	ICEServers: []webrtc.ICEServer{},
	// }
	peerConnectionConfig := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}

	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		panic(err)
	}

	// Create a InterceptorRegistry. This is the user configurable RTP/RTCP Pipeline.
	// This provides NACKs, RTCP Reports and other features. If you use `webrtc.NewPeerConnection`
	// this is enabled by default. If you are manually managing You MUST create a InterceptorRegistry
	// for each PeerConnection.
	interceptorRegistry := &interceptor.Registry{}

	messageChannel := make(chan []byte)
	// Use the default set of Interceptors
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptorRegistry); err != nil {
		panic(err)
	}

	// Register a intervalpli factory
	// This interceptor sends a PLI every 3 seconds. A PLI causes a video keyframe to be generated by the sender.
	// This makes our video seekable and more error resilent, but at a cost of lower picture quality and higher bitrates
	// A real world application should process incoming RTCP packets from viewers and forward them to senders
	intervalPliFactory, err := intervalpli.NewReceiverInterceptor()
	if err != nil {
		panic(err)
	}
	interceptorRegistry.Add(intervalPliFactory)

	statsInterceptorFactory, err := stats.NewInterceptor()
	if err != nil {
		panic(err)
	} // to connected peers

	var statsGetter stats.Getter
	statsInterceptorFactory.OnNewPeerConnection(func(_ string, g stats.Getter) {
		fmt.Println("connected!!!")
		statsGetter = g
	})

	interceptorRegistry.Add(statsInterceptorFactory)

	// Create a new RTCPeerConnection
	peerConnection, err := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
	).NewPeerConnection(peerConnectionConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if cErr := peerConnection.Close(); cErr != nil {
			fmt.Printf("cannot close peerConnection: %v\n", cErr)
		}
	}()

	// Allow us to receive 1 video track
	if _, err = peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo); err != nil {
		panic(err)
	}

	localTrackChan := make(chan *webrtc.TrackLocalStaticRTP)
	// Set a handler for when a new remote track starts, this just distributes all our packets

	peerConnection.OnTrack(func(remoteTrack *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) { //nolint: revive
		// Create a local track, all our SFU clients will be fed via this track
		fmt.Println("on_track")
		// go func() {
		// 	buf := make([]byte, 1500) // RTP Packet Buffer
		// 	for {
		// 		// Set a timeout for reading RTP packets (to prevent buffering delay)
		// 		remoteTrack.SetReadDeadline(time.Now().Add(300 * time.Millisecond))

		// 		// Read RTP packet
		// 		_, _, err := remoteTrack.Read(buf)
		// 		if err != nil {
		// 			fmt.Println("RTP Read error:", err)
		// 			return
		// 		}

		// 		fmt.Println("Received RTP packet")
		// 	}
		// }()
		localTrack, newTrackErr := webrtc.NewTrackLocalStaticRTP(remoteTrack.Codec().RTPCodecCapability, "video", "pion")
		if newTrackErr != nil {
			panic(newTrackErr)
		}

		localTrackChan <- localTrack

		go func() {
			// Print the stats for this individual track
			for {
				stats := statsGetter.Get(uint32(remoteTrack.SSRC()))

				// fmt.Printf("Stats for: %s\n", remoteTrack.Codec().MimeType)
				// fmt.Println(stats.InboundRTPStreamStats)
				// fmt.Println(stats.RemoteOutboundRTPStreamStats)

				// fmt.Println("-----", stats.InboundRTPStreamStats.PacketsReceived, "-----")
				webrtcStats.PacketsReceived.WithLabelValues("PacketsReceived").Add(float64(stats.InboundRTPStreamStats.PacketsReceived))
				webrtcStats.PacketsLost.WithLabelValues("PacketsLost").Add(float64(stats.InboundRTPStreamStats.PacketsLost))
				webrtcStats.Jitter.WithLabelValues("Jitter").Set(stats.InboundRTPStreamStats.Jitter)
				webrtcStats.BytesReceived.WithLabelValues("BytesReceived").Add(float64(stats.InboundRTPStreamStats.BytesReceived))
				webrtcStats.HeaderBytesReceived.WithLabelValues("HeaderBytesReceived").Add(float64(stats.InboundRTPStreamStats.HeaderBytesReceived))
				webrtcStats.FIRCount.WithLabelValues("FIRCount").Add(float64(stats.InboundRTPStreamStats.FIRCount))
				webrtcStats.PLICount.WithLabelValues("PLICount").Add(float64(stats.InboundRTPStreamStats.PLICount))
				webrtcStats.NACKCount.WithLabelValues("NACKCount").Add(float64(stats.InboundRTPStreamStats.NACKCount))

				time.Sleep(time.Second * 1)
			}
		}()

		rtpBuf := make([]byte, 1400)
		for {
			i, _, readErr := remoteTrack.Read(rtpBuf)
			if readErr != nil {
				panic(readErr)
			}

			// ErrClosedPipe means we don't have any subscribers, this is ok if no peers have connected yet
			if _, err = localTrack.Write(rtpBuf[:i]); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				panic(err)
			}
		}
	})
	// ---------------------------------------------------------------------------------------------------------------------------

	var iceConnectionState atomic.Value
	iceConnectionState.Store(webrtc.ICEConnectionStateNew)

	// Set the handler for ICE connection state
	// This will notify you when the peer has connected/disconnected
	peerConnection.OnICEConnectionStateChange(func(connectionState webrtc.ICEConnectionState) {
		fmt.Printf("Connection State has changed %s \n", connectionState.String())
		iceConnectionState.Store(connectionState)
	})

	peerConnection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			fmt.Println("Discovered ICE Candidate:", candidate.Address)
		}
	})

	Ordered := true
	sendChannel, err := peerConnection.CreateDataChannel("Joystick-signal-reciever", &webrtc.DataChannelInit{Ordered: &Ordered})

	if err != nil {
		return
	}

	sendChannel.OnClose(func() {
		fmt.Println("Channel closed")
	})

	sendChannel.OnOpen(func() {
		fmt.Println("Data channel opened")
	})

	sendChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		fmt.Printf(" msg recieved: %s \n", msg.Data)
	})

	go func() {
		for {
			sendChannel.Send(<-messageChannel)

		}
	}()

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

	localdescription := encode(peerConnection.LocalDescription())

	resp, err := http.Post("https://webrtc2.hopto.org:8082/offer", "text/plain", bytes.NewBuffer([]byte(localdescription)))
	if err != nil {
		panic(err)
	}
	// defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	answer := webrtc.SessionDescription{}
	decode(string(body), &answer)

	// Set the remote SessionDescription
	err = peerConnection.SetRemoteDescription(answer)
	if err != nil {
		panic(err)
	}

	// ---------------------------------------------------------------------------------------------------------------------------------

	// Set the remote SessionDescription
	// err = peerConnection.SetRemoteDescription(offer)
	// if err != nil {
	// 	panic(err)
	// }

	// // Create answer
	// answer, err := peerConnection.CreateAnswer(nil)
	// if err != nil {
	// 	panic(err)
	// }

	// // Create channel that is blocked until ICE Gathering is complete
	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	// // Sets the LocalDescription, and starts our UDP listeners
	// err = peerConnection.SetLocalDescription(answer)
	// if err != nil {
	// 	panic(err)
	// }

	// // Block until ICE Gathering is complete, disabling trickle ICE
	// // we do this because we only can exchange one signaling message
	// // in a production application you should exchange ICE Candidates via OnICECandidate
	// fmt.Println("before")

	// <-gatherComplete

	// fmt.Println("after")

	// // Get the LocalDescription and take it to base64 so we can paste in browser
	// // fmt.Println(encode(peerConnection.LocalDescription()))
	// ch <- encode(peerConnection.LocalDescription())
	// -------------------------------------------
	localTrack := <-localTrackChan
	for {
		fmt.Println("")
		fmt.Println("Curl an base64 SDP to start sendonly peer connection")
		recvOnlyOffer := webrtc.SessionDescription{}
		decode(<-sdpChan, &recvOnlyOffer)

		// Create a new PeerConnection
		peerConnection, err := webrtc.NewPeerConnection(peerConnectionConfig)
		if err != nil {
			panic(err)
		}

		rtpSender, err := peerConnection.AddTrack(localTrack)
		if err != nil {
			fmt.Println("err add track")
			panic(err)
		}

		// Read incoming RTCP packets
		// Before these packets are returned they are processed by interceptors. For things
		// like NACK this needs to be called.
		go func() {
			rtcpBuf := make([]byte, 1500)
			for {
				if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
					return
				}
			}
		}()

		peerConnection.OnDataChannel(func(dataChannel *webrtc.DataChannel) {
			fmt.Printf("New DataChannel %s %d\n", dataChannel.Label(), dataChannel.ID())

			// Register channel opening handling
			dataChannel.OnOpen(func() {
				NewHybridClock := fastclock.NewHybridClock()
				_ = NewHybridClock
				var lastMessageTime atomic.Value
				lastMessageTime.Store(time.Now())
				fmt.Printf(
					"Data channel '%s'-'%d' open. Random messages will now be sent to any connected DataChannels every 5 seconds\n",
					dataChannel.Label(), dataChannel.ID(),
				)

				fmt.Println("sendChannel has opened")
				frameID := 0

				ticker := time.NewTicker(100 * time.Millisecond)

				go func() {
					for range ticker.C {
						// response, error := ntp.Query("0.beevik-ntp.pool.ntp.org")
						// if error != nil {
						// 	fmt.Println("Error querying NTP server:", error)
						// 	continue
						// }

						// time_now := time.Now()
						// messageSentTime := time_now.Add(response.ClockOffset).UnixMilli()
						// messageSentTime := time.Now().UnixMilli()
						messageSentTimePrev := lastMessageTime.Load().(time.Time).UnixMilli()
						messageSentTime := time.Now().UnixMilli()
						sent_rate := float64(messageSentTime - messageSentTimePrev) // in milliseconds
						webrtcStats.MessageSendRate.WithLabelValues("MessageSendRate").Set(sent_rate)
						// Update the last message time
						lastMessageTime.Store(time.Now())
						// fmt.Printf("%s, %s, %s \n", strconv.FormatInt(messageSentTime, 10), response.ClockOffset, response.Time) // Equivalent to JS Date.now()
						payloadBytes := make([]byte, 120000) // 120000 bytes = 120 KB payload
						_, err := rand.Read(payloadBytes)
						if err != nil {
							fmt.Println("Error generating random payload:", err)
							continue
						}

						// Encode to base64 to make it JSON-safe (you can also use hex if needed)
						payload := base64.StdEncoding.EncodeToString(payloadBytes)

						message := DataChannelMessage{
							FrameID:                int64(frameID),
							MessageSentTimeClient2: messageSentTime,
							MessageSendRate:        int64(sent_rate),
							Payload:                []byte(payload),
						}

						fmt.Printf("Sending message with FrameID: %d, MessageSentTimeClient2: %d\n", message.FrameID, message.MessageSentTimeClient2)

						data, err := json.Marshal(message)
						if err != nil {
							fmt.Println("Error marshaling message:", err)
							continue
						}

						messageChannel <- data
						// err = dataChannel.Send(data)
						// if err != nil {
						// 	fmt.Println("Error sending message:", err)
						// }

						frameID++
					}
				}()
			})

			// Register text message handling
			dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
				// fmt.Printf("Message from DataChannel '%s': '%s'\n", dataChannel.Label(), string(msg.Data))
				// messageChannel <- msg.Data
			})
		})

		// Set the remote SessionDescription
		err = peerConnection.SetRemoteDescription(recvOnlyOffer)
		if err != nil {
			fmt.Println("err SetRemoteDescription")

			panic(err)
		}

		// Create answer
		answer, err := peerConnection.CreateAnswer(nil)
		if err != nil {
			fmt.Println("err CreateAnswer")

			panic(err)
		}

		// Create channel that is blocked until ICE Gathering is complete
		gatherComplete = webrtc.GatheringCompletePromise(peerConnection)

		// Sets the LocalDescription, and starts our UDP listeners
		err = peerConnection.SetLocalDescription(answer)
		if err != nil {
			panic(err)
		}

		// Block until ICE Gathering is complete, disabling trickle ICE
		// we do this because we only can exchange one signaling message
		// in a production application you should exchange ICE Candidates via OnICECandidate
		<-gatherComplete

		// Get the LocalDescription and take it to base64 so we can paste in browser
		// fmt.Println(encode(peerConnection.LocalDescription()))
		// send to channel A
		fmt.Println("waiting to send")

		ch <- encode(peerConnection.LocalDescription())

	}
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
	// fmt.Printf(" incoming data %#v", in)
	b, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		panic(err)
	}

	if err = json.Unmarshal(b, obj); err != nil {
		panic(err)
	}
}

// httpSDPServer starts a HTTP Server that consumes SDPs.
func httpSDPServer(port int, ch chan string) chan string {
	sdpChan := make(chan string)

	mux_s1 := http.NewServeMux()
	mux_s1.Handle("/metrics", promhttp.Handler())
	mux_s1.HandleFunc("/offer", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Access-Control-Allow-Origin", "*")
		res.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		res.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight request
		if req.Method == "OPTIONS" {
			res.WriteHeader(http.StatusOK)
			return
		}

		if req.Method != http.MethodPost {
			http.Error(res, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(req.Body)
		sdpChan <- string(body)
		// recieve from channel A
		response_string := <-ch
		// fmt.Fprintf(res, response_string) //nolint: errcheck
		// fmt.Printf("%+v", response_string)
		res.Header().Set("Content-Type", "text/plain")
		res.Write([]byte(response_string))

	})

	go func() {
		// nolint: gosec
		panic(http.ListenAndServe(":"+strconv.Itoa(port), mux_s1))
	}()

	return sdpChan
}

func httpStaticServer(port int) {
	mux_s2 := http.NewServeMux()
	fs := http.FileServer(http.Dir("static"))
	mux_s2.Handle("/", fs)

	go func() {
		// nolint: gosec
		panic(http.ListenAndServe(":"+strconv.Itoa(port), mux_s2))
	}()
}
