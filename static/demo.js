/* eslint-env browser */

// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

const log = msg => {
    document.getElementById('logs').innerHTML += msg + '<br>'
  }
  
  window.createSession = isPublisher => {
    const pc = new RTCPeerConnection({
      iceServers: [
        {
          urls: 'stun:stun.l.google.com:19302'
        },
        {urls:"turn:global.relay.metered.ca:80",username:"e7c2418ad54a28c683cde02e",credential:"ui+6iGFVbG7OlBIP"}
      ]
    })
    // const pc = new RTCPeerConnection({})
    pc.oniceconnectionstatechange = e => log(pc.iceConnectionState)
    pc.onicecandidate = event => {
      if (event.candidate === null) {
        document.getElementById('localSessionDescription').value = btoa(JSON.stringify(pc.localDescription))
        console.log(pc.localDescription);

        fetch('http://localhost:8082/offer', {
          method: 'POST',
          headers: { 'Content-Type': 'application/text' },
          body: btoa(JSON.stringify(pc.localDescription))
      })
      .then(response => response.text())
      .then(encodedAnswer => {
          console.log('Received answer:', encodedAnswer);
  
          document.getElementById('remoteSessionDescription').value = encodedAnswer
          window.startSession()
      })
      .catch(error => console.error('Error:', error));
      }
    }

    let sendChannel = pc.createDataChannel('Joystick-signal')
    sendChannel.onclose = () => console.log('sendChannel has closed')
    sendChannel.onopen = () => {
    console.log('sendChannel has opened');
    let frameId = 0;
  setInterval(() => {
    const timestamp = Date.now();
    sendChannel.send(JSON.stringify({ frameId, timestamp }));
    frameId++;
  }, 1000); // Simulate 100fps video stream

  // Setup listeners only once the channel is open
  document.querySelectorAll('.button').forEach(btn => {
    btn.addEventListener('click', () => {
      if (sendChannel.readyState === 'open') {
        sendChannel.send(`Button ${btn.id} pressed`);
        console.log(`Button ${btn.id} pressed`);
      }
    });
  });
};
sendChannel.onmessage = e => log(`Message from DataChannel '${sendChannel.label}' payload '${String(e.data)}'`)
  
    if (isPublisher) {
      navigator.mediaDevices.getUserMedia({ video: true, audio: false })
        .then(stream => {
          stream.getTracks().forEach(track => pc.addTrack(track, stream))
          document.getElementById('video1').srcObject = stream
          pc.createOffer()
            .then(d => {
              pc.setLocalDescription(d)
              console.log("localdescription set");
              
            })
            .catch(log)
        }).catch(log)
    } else {
      pc.addTransceiver('video')
      pc.createOffer()
        .then(d => pc.setLocalDescription(d))
        .catch(log)
  
      pc.ontrack = function (event) {
        const el = document.getElementById('video1')
        el.srcObject = event.streams[0]
        el.autoplay = true
        el.controls = true
      }
    }
  
    window.startSession = () => {
      const sd = document.getElementById('remoteSessionDescription').value
      if (sd === '') {
        return alert('Session Description must not be empty')
      }
  
      try {
        pc.setRemoteDescription(JSON.parse(atob(sd)))
      } catch (e) {
        alert(e)
      }
    }
  
    window.copySDP = () => {
      const browserSDP = document.getElementById('localSessionDescription')
  
      browserSDP.focus()
      browserSDP.select()
  
      try {
        const successful = document.execCommand('copy')
        const msg = successful ? 'successful' : 'unsuccessful'
        log('Copying SDP was ' + msg)
      } catch (err) {
        log('Unable to copy SDP ' + err)
      }
    }
  
    const btns = document.getElementsByClassName('createSessionButton')
    for (let i = 0; i < btns.length; i++) {
      btns[i].style = 'display: none'
    }
  
    document.getElementById('signalingContainer').style = 'display: block'
  }


