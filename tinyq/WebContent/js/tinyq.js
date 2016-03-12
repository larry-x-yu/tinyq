var TinyQ = (function() {
	"use strict";
	var subscriptions = {};
	var subId = 0;
	var brokerUrl = null;
	var _socket = null;

	var dispatch = function(data) {
		var nvpairs = {};
		if(data) {
			var components = data.split("&");
			for(var i in components) {
				var parts = components[i].split("=");
				nvpairs[parts[0]] = parts[1];
			}
		}
		
		if(nvpairs["action"] === "pub") {
			var subs = subscriptions[nvpairs["topic"]];
			if(subs) {
					for(var j in subs) {
						try {
							subs[j].call(null, nvpairs["topic"], nvpairs["message"]);
						}
						catch(e) {
							console.log("Error dispatching message to subscriber: " + j);
						}
					}
			}
		}			
	};
	
	return {
		_getSubId : function(topic) {
			return topic + "_" + subId++;
		},

		_getTopic : function(mySubId) {
			var topic = "";
			if (!mySubId) {
				topic = mySubId.slice(0, mySubId.indexOf(':'));
			}
			return topic;
		},

		_isEmpty : function(obj) {
			for ( var prop in obj) {
				if (obj.hasOwnProperty(prop))
					return false;
			}

			return true;
		},

		/* We do lazy connection */
		connect : function(serverBrokerUrl) {
			brokerUrl = serverBrokerUrl;
		},

		_connect : function() {
			return new Promise(
					function(resolve, reject) {
						
						debugger;
						
						if (!brokerUrl) {
							reject("brokerUrl empty, call 'connect()' first");
							return;
						}

						if (_socket) {
							resolve(_socket);
							return;
						}

						if ('WebSocket' in window) {
							_socket = new WebSocket(brokerUrl);
						} else if ('MozWebSocket' in window) {
							_socket = new MozWebSocket(brokerUrl);
						} else {
							_socket = null;
							reject('Error: WebSocket is not supported by this browser.');
						}

						// Add default listeners
						_socket.onopen = function() {
							console.debug('WebSocket connection opened: ' + brokerUrl);
							resolve(_socket);
						};

						_socket.onclose = function() {
							_socket = null;
							console.debug('WebSocket closed: ' + brokerUrl);
							reject('WebSocket closed: ' + brokerUrl);
						};

						_socket.onerror = function(err) {
							_socket = null;
							console.debug('WebSocket error: ' + err);
							reject(err);
						};

						_socket.onmessage = function(message) {							
							console.log(message.data);
							dispatch(message.data);
							
						};						
					});
		},
		
		// close : function() {
		// if (socket) {
		// socket.close();
		// socket = null;
		// }
		// subscriptions = {};
		// },

		subscribe : function(topic, callback) {
			var currentSubscriptions = subscriptions[topic];
			if (!currentSubscriptions) {
				currentSubscriptions = {};
			}

			if (this._isEmpty(currentSubscriptions)) {
				this.send("action=sub&topic=" + topic);
			}

			var mySubId = this._getSubId(topic);
			currentSubscriptions[mySubId] = callback;
			subscriptions[topic] = currentSubscriptions;

			return mySubId;
		},

		unsubscribe : function(id) {
			var topic = this._getTopic(id);
			var subs = subscriptions[topic];
			if (subs) {
				delete subs[id];
				subscripitons[topic] = subs;

				if (this._isEmpty(subs)) {
					this.send("action=unsub&topic=" + topic);
				}
			}
		},

		publish : function(topic, message) {
			this.send("action=pub&topic=" + topic + "&message=" + message);
		},

		send : function(message) {
			this._connect().then(function(socket) {
				debugger;
				socket.send(message);
			}, function(err) {
				console.log("Error when sending message: " + message);
			});
		}
	};
}());