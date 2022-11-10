*
 * Copyright 2014 IBM Corp.
 * Copyright 2022 BCC Media Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

const s3SDK = require("@aws-sdk/client-s3");

module.exports = function(RED) {
	"use strict";
	var fs = require('fs');
	//var minimatch = require("minimatch");

	function AWSNode(n) {
		RED.nodes.createNode(this,n);
		if (
			this.credentials &&
			this.credentials.accesskeyid &&
			this.credentials.secretaccesskey
		) {
			const credentials = {
				accessKeyId: this.credentials.accesskeyid,
				secretAccessKey: this.credentials.secretaccesskey,
			}
			this.S3 = new s3SDK.S3({
				region: n.region,
				credentials,
			});
		}
	}

	RED.nodes.registerType("aws-config",AWSNode,{
		credentials: {
			accesskeyid: { type:"text" },
			secretaccesskey: { type: "password" }
		}
	});

	/*
	function AmazonS3InNode(n) {
		RED.nodes.createNode(this,n);
		this.awsConfig = RED.nodes.getNode(n.aws);
		// eu-west-1||us-east-1||us-west-1||us-west-2||eu-central-1||ap-northeast-1||ap-northeast-2||ap-southeast-1||ap-southeast-2||sa-east-1
		this.region = n.region || "eu-west-1";
		this.bucket = n.bucket;
		this.filepattern = n.filepattern || "";
		var node = this;
		var AWS = this.awsConfig ? this.awsConfig.AWS : null;

		if (!AWS) {
			node.warn(RED._("aws.warn.missing-credentials"));
			return;
		}
		var s3 = new AWS.S3({"region": node.region});
		node.status({fill:"blue",shape:"dot",text:"aws.status.initializing"});
		var contents = [];
		node.listAllObjects(s3, { Bucket: node.bucket },contents, function(err, data) {
			if (err) {
				node.error(RED._("aws.error.failed-to-fetch", {err:AWS}));
				node.status({fill:"red",shape:"ring",text:"aws.status.error"});
				return;
			}
			var contents = node.filterContents(data);
			node.state = contents.map(function (e) { return e.Key; });
			node.status({});
			node.on("input", function(msg) {
				node.status({fill:"blue",shape:"dot",text:"aws.status.checking-for-changes"});
				var contents = [];
				node.listAllObjects(s3, { Bucket: node.bucket }, contents, function(err, data) {
					if (err) {
						node.error(RED._("aws.error.failed-to-fetch", {err:err}),msg);
						node.status({});
						return;
					}
					node.status({});
					var newContents = node.filterContents(data);
					var seen = {};
					var i;
					msg.bucket = node.bucket;
					for (i = 0; i < node.state.length; i++) {
						seen[node.state[i]] = true;
					}
					for (i = 0; i < newContents.length; i++) {
						var file = newContents[i].Key;
						if (seen[file]) {
							delete seen[file];
						} else {
							var newMessage = RED.util.cloneMessage(msg);
							newMessage.payload = file;
							newMessage.file = file.substring(file.lastIndexOf('/')+1);
							newMessage.event = 'add';
							newMessage.data = newContents[i];
							node.send(newMessage);
						}
					}
					for (var f in seen) {
						if (seen.hasOwnProperty(f)) {
							var newMessage = RED.util.cloneMessage(msg);
							newMessage.payload = f;
							newMessage.file = f.substring(f.lastIndexOf('/')+1);
							newMessage.event = 'delete';
// newMessage.data intentionally null
							node.send(newMessage);
						}
					}
					node.state = newContents.map(function (e) {return e.Key;});
				});
			});
			var interval = setInterval(function() {
				node.emit("input", {});
			}, 900000); // 15 minutes
			node.on("close", function() {
				if (interval !== null) {
					clearInterval(interval);
				}
			});
		});
	}
	RED.nodes.registerType("amazon s3 in", AmazonS3InNode);

	AmazonS3InNode.prototype.listAllObjects = function(s3, params, contents, cb) {
		var node = this;
		s3.listObjects(params, function(err, data) {
			if (err) {
				cb(err, contents);
			} else {
				contents = contents.concat(data.Contents);
				if (data.IsTruncated) {
					// Set Marker to last returned key
					params.Marker = contents[contents.length-1].Key;
					node.listAllObjects(s3, params, contents, cb);
				} else {
					cb(err, contents);
				}
			}
		});
	};

	AmazonS3InNode.prototype.filterContents = function(contents) {
		var node = this;
		return node.filepattern ? contents.filter(function (e) {
			return minimatch(e.Key, node.filepattern);
		}) : contents;
	};

	function AmazonS3QueryNode(n) {
		RED.nodes.createNode(this,n);
		this.awsConfig = RED.nodes.getNode(n.aws);
		this.region = n.region || "eu-west-1";
		this.bucket = n.bucket;
		this.filename = n.filename || "";
		var node = this;
		var AWS = this.awsConfig ? this.awsConfig.AWS : null;

		if (!AWS) {
			node.warn(RED._("aws.warn.missing-credentials"));
			return;
		}
		var s3 = new AWS.S3({"region": node.region});
		node.on("input", function(msg) {
			var bucket = node.bucket || msg.bucket;
			if (bucket === "") {
				node.error(RED._("aws.error.no-bucket-specified"),msg);
				return;
			}
			var filename = node.filename || msg.filename;
			if (filename === "") {
				node.warn("No filename");
				node.error(RED._("aws.error.no-filename-specified"),msg);
				return;
			}
			msg.bucket = bucket;
			msg.filename = filename;
			node.status({fill:"blue",shape:"dot",text:"aws.status.downloading"});
			s3.getObject({
				Bucket: bucket,
				Key: filename,
			}, function(err, data) {
				if (err) {
					node.warn(err);
					node.error(RED._("aws.error.download-failed",{err:err.toString()}),msg);
					return;
				} else {
					msg.payload = data.Body;
				}
				node.status({});
				node.send(msg);
			});
		});
	}
	RED.nodes.registerType("amazon s3", AmazonS3QueryNode);
	*/

					function AmazonS3OutNode(n) {
						RED.nodes.createNode(this,n);
						this.awsConfig = RED.nodes.getNode(n.aws);
						this.region = n.region  || "eu-west-1";
						this.bucket = n.bucket;
						this.filename = n.filename || "";
						this.localFilename = n.localFilename || "";
						var node = this;

						if (!this.awsConfig.S3) {
							node.warn("Missing credentials");
							node.status({fill:"red",shape:"dot",text:"Missing credentials"});
							return;
						}

						const s3 = this.awsConfig.S3;

						node.status({fill:"blue",shape:"dot",text:"aws.status.checking-credentials"});
						/// TODO: Promises man, promises
						s3.headBucket({ Bucket: node.bucket }, function(err) {
							if (err) {
								node.error(err);
								node.status({fill:"red",shape:"ring",text:"Error. See Log"});
								return;
							}
							node.status({fill:"green",shape:"square",text:"Ready"});
							node.on("input", function(msg) {
								var bucket = node.bucket || msg.bucket;
								if (bucket === "") {
									node.error("Missing bucket")
									return;
								}
								var filename = node.filename || msg.filename;
								if (filename === "") {
									node.error("Missing file name")
									return;
								}
								var localFilename = node.localFilename || msg.localFilename;
								if (localFilename) {
									// TODO: use chunked upload for large files
									node.status({fill:"blue",shape:"dot",text:"Uploading"});
									var stream = fs.createReadStream(localFilename);
									s3.putObject({
										Bucket: bucket,
										Body: stream,
										Key: filename,
									}, function(err) {
										if (err) {
											node.error(err.toString(),msg);
											node.status({fill:"red",shape:"ring",text:"Failed"});
											return;
										}
										node.status({fill:"green",shape:"ring",text:"Done"});
									});
								} else if (typeof msg.payload !== "undefined") {
									node.status({fill:"blue",shape:"dot",text:"Uploading"});
									s3.putObject({
										Bucket: bucket,
										Body: RED.util.ensureBuffer(msg.payload),
										Key: filename,
									}, function(err) {
										if (err) {
											node.error(err.toString(),msg);
											node.status({fill:"red",shape:"ring",text:"Failed"});
											return;
										}
										node.status({fill:"green",shape:"ring",text:"Done"});
									});
								}
							});
						});
					}
	RED.nodes.registerType("amazon s3 out",AmazonS3OutNode);
};

