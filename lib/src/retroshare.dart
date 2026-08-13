// ignore_for_file: constant_identifier_names, avoid_print, comment_references

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:retroshare_api_wrapper/retroshare.dart';
import 'package:retroshare_api_wrapper/src/rs_models.dart';

String? _retroshareServicePrefix; // Used for remote control feature

const RETROSHARE_HOST = '127.0.0.1';
const RETROSHARE_PORT = 9092;
const RETROSHARE_SERVICE_PREFIX = 'http://$RETROSHARE_HOST:$RETROSHARE_PORT';

const RS_MSG_PENDING = 0x0002;

const RETROSHARE_CHANNEL_NAME = 'cc.retroshare.retroshare/retroshare';

void dbg(String msg) {
  stderr.writeln(msg);
}

String errToStr(Map<String, dynamic> cxxStdErrorCondition) {
  final err = cxxStdErrorCondition;
  return "${err["errorCategory"]} ${err["errorNumber"]} ${err["errorMessage"]}";
}

/// Returns an authentication header to use for RS
String makeAuthHeader(String username, String password) =>
    'Basic ${base64Encode(utf8.encode('$username:$password'))}';

void setRetroshareServicePrefix([String prefix = RETROSHARE_SERVICE_PREFIX]) =>
    _retroshareServicePrefix = prefix;

String getRetroshareServicePrefix() =>
    _retroshareServicePrefix ?? RETROSHARE_SERVICE_PREFIX;

// ////////////////////////////////////////////////////////////////////////////
// / SERVICE LIFECYCLE MANAGEMENT
// ////////////////////////////////////////////////////////////////////////////

/// Set internal var

/// Check if RS is running, try to restart it otherwise
///
/// It try to restart it using the [rsStartCallback] that have to be set before
/// using [setStartCallback].
///
/// Return true if success throw exception if not.
Future<bool> restartRSIfDown() async {
  if (!(await isRetroshareRunning())) {
    print('RS service is down. Restarting');
    try {
      await rsStartCallback();
    } catch (error) {
      throw Exception('Failed to start RS service. $error');
    }
  }
  return true;
}

/// Function to check if Retroshare is running
///
/// Return true if receive a response from the server, false otherwise.
Future<bool> isRetroshareRunning() async {
  final reqUrl = getRetroshareServicePrefix();
  try {
    // If the server responds at all (even with 401 or 404), it means the process is running.
    final response = await http.get(Uri.parse('$reqUrl/rsJsonApi/version')).timeout(const Duration(seconds: 2));
    return response.statusCode != 0; 
  } catch (err) {
    return false;
  }
}

// ////////////////////////////////////////////////////////////////////////////
// / RS EVENTS
// ////////////////////////////////////////////////////////////////////////////
class RsEvents {
  static Map<RsEventType, StreamSubscription<String>>? rsEventsSubscriptions;

  /// Register Event
  ///
  /// Where [eventType] is the enum type [RsEventType] that specifies what kind
  /// of event are we listening to.
  ///
  /// The callback return this StreamSubscription object and the Json response
  static Future<StreamSubscription<String>?> registerEventsHandler(
    RsEventType eventType,
    Function callback,
    AuthToken? authToken, {
    required Function? onError,
    String? basicAuth,
  }) async {
    await restartRSIfDown();

    final body = {'eventType': eventType.index};
    const path = '/rsEvents/registerEventsHandler';
    final reqUrl = getRetroshareServicePrefix() + path;
    StreamSubscription<String>? streamSubscription;
    http.Client? client; // Keep a reference to the client to close it later

    try {
      client = http.Client();
      final request = http.Request('GET', Uri.parse(reqUrl));
      request.headers.addAll({
        HttpHeaders.authorizationHeader:
            'Basic ${base64.encode(utf8.encode(authToken.toString()))}',
        HttpHeaders.contentTypeHeader: 'application/json',
        HttpHeaders.acceptHeader: 'text/event-stream', // Important for SSE
        HttpHeaders.cacheControlHeader: 'no-cache', // Important for SSE
      });
      request.body = jsonEncode(body);

      final response = await client.send(request);

      if (response.statusCode == 200) {
        streamSubscription = response.stream
            .transform(utf8.decoder) // Decode bytes to UTF-8 string
            .transform(const LineSplitter()) // Split stream into lines
            .listen((String line) {
          // Basic SSE parsing: look for lines starting with "data: "
          if (line.startsWith('data: ')) {
            final eventData = line.substring(6).trim(); // Get data part
            if (eventData.isNotEmpty) {
              try {
                final jsonData = jsonDecode(eventData);
                if (jsonData['event'] != null) {
                  // Pass the subscription and the event data to the callback
                  callback(streamSubscription, jsonData['event']);
                }
              } catch (e) {
                print('Error decoding JSON from SSE event: $e');
                print('Received data: $eventData');
                // Optionally call onError callback or handle differently
              }
            }
          }
        })
          ..onError((error, stackTrace) {
            print('SSE Stream Error: $error');
            if (onError != null) {
              onError(error, stackTrace);
            }
            client?.close(); // Close the client on stream error
          })
          ..onDone(() {
            print('SSE Stream Closed');
            client?.close(); // Close the client when the stream is done
          });
      } else {
        // Handle non-200 status code
        print(
          'Failed to connect to SSE stream. Status code: ${response.statusCode}',
        );
        client.close();
        // Throw specific error or call onError based on status code
        throw statusCodeErrorMessages(response.statusCode, path, reqUrl);
      }
    } on SocketException catch (e) {
      // Handle network errors (e.g., connection refused)
      print('registerEventsHandler SocketException: $e');
      client?.close();
      if (onError != null) {
        onError(e, StackTrace.current);
      }
      // Rethrow or handle as appropriate
      rethrow;
    } catch (e, stackTrace) {
      // Catch other potential errors during setup or listening
      print('registerEventsHandler generic error: $e');
      client?.close();
      if (onError != null) {
        onError(e, stackTrace);
      }
      // Rethrow or handle as appropriate
      rethrow;
    }

    // Store the subscription on a dictionary
    rsEventsSubscriptions ??= <RsEventType, StreamSubscription<String>>{};
    if (rsEventsSubscriptions != null) {
      // Check if streamSubscription is not null before assigning
      rsEventsSubscriptions![eventType] = streamSubscription;
    } else {
      // Handle the case where rsEventsSubscriptions is null or streamSubscription is null
      // This might happen if connection failed before the stream was established
    }
    return streamSubscription;
  }
}

// ////////////////////////////////////////////////////////////////////////////
// / RAW MESSAGE PASSING
// ////////////////////////////////////////////////////////////////////////////

/// Return diferent Strings to throw depending the status code
String statusCodeErrorMessages(int statusCode, String path, String reqUrl) {
  switch (statusCode) {
    case 401:
      return 'Authentication failed';
    case 404:
      return 'Method not found: $path';
    default:
      throw ApiUnhandledErrorException(statusCode, reqUrl);
  }
}

/// Exception thrown when the API error is not handled yet
///
/// If the API error is not handled, throw this exception. Check
/// [statusCodeErrorMessages] to see handled exceptions.
class ApiUnhandledErrorException implements Exception {
  ApiUnhandledErrorException(int statusCode, String reqUrl) {
    _message = 'Unhandled statusCode: $statusCode $reqUrl';
  }
  String? _message;
  @override
  String toString() {
    return _message ?? ''; // Return empty string if null
  }
}

class LoginException implements Exception {
  final String _message = 'Please, log in first ...';
  @override
  String toString() => _message;
}

dynamic rsStartCallback;

void setStartCallback(dynamic callback) {
  rsStartCallback = callback;
}

/// Recursively removes null values from a map or list
dynamic _stripNulls(dynamic item) {
  if (item is Map) {
    return Map.fromEntries(
      item.entries
          .where((e) => e.value != null)
          .map((e) => MapEntry(e.key, _stripNulls(e.value))),
    );
  } else if (item is List) {
    return item.where((v) => v != null).map(_stripNulls).toList();
  }
  return item;
}

/// Call the given RetroShare JSON API method with given paramethers, and return
/// results, raise exceptions on errors.
/// Path is expected to contain a leading slash "/" and params is expected to
/// be serializable to JSON.
Future<Map<String, dynamic>> rsApiCall(
  String path, {
  AuthToken? authToken,
  Map<String, dynamic>? params,
  http.Client? client,
}) async {
  final httpClient = client ?? http.Client();
  try {
    final reqUrl = '${getRetroshareServicePrefix()}$path';
    final Map<String, String> headers = {
      HttpHeaders.contentTypeHeader: 'application/json',
    };
    if (authToken != null) {
      headers[HttpHeaders.authorizationHeader] =
          'Basic ${base64.encode(utf8.encode(authToken.toString()))}';
    }

    // Crucial: Strip nulls from params as RS C++ engine rejects them
    final cleanParams = _stripNulls(params ?? {});

    final response = await httpClient.post(
      Uri.parse(reqUrl),
      body: jsonEncode(cleanParams),
      headers: headers,
    );

    if (response.statusCode == 200) {
      // Revert: Do not automatically check retval here.
      // Let the calling method decide how to interpret the response body.
      return jsonDecode(response.body);
    } else {
      print('DEBUG: rsApiCall failed. Path: $path, Status: ${response.statusCode}, Body: ${response.body}');
      // Throw for non-200 status codes
      throw Exception(
        'HTTP Error: Status code ${response.statusCode} for path $path',
      );
    }
  } on SocketException {
    if (client == null && await restartRSIfDown()) {
      return rsApiCall(
        path,
        params: params,
        authToken: authToken,
      ); // No client passed on retry
    } else {
      throw SocketException(
        'Retroshare service is down or unreachable (Path: $path).',
      );
    }
  } catch (err) {
    rethrow;
  } finally {
    if (client == null) {
      httpClient.close();
    }
  }
}

// ////////////////////////////////////////////////////////////////////////////
// / SPECIFIC RETROSHARE OPERATIONS
// ////////////////////////////////////////////////////////////////////////////

class RsAccounts {
  static Future<String?> getCurrentAccountId(AuthToken authToken) async {
    try {
      const mPath = '/rsAccounts/getCurrentAccountId';
      final response = await rsApiCall(mPath, authToken: authToken);
      final retval = response['retval'];
      if ((retval is bool && retval) || (retval is int && retval == 1)) {
        return response['id'];
      }
      return null;
    } catch (err) {
      throw Exception('something went wrong!');
    }
  }

  static Future<dynamic> importIdentity(String base64Data) async {
    final mParams = {
      'id_data': base64Data,
    };
    try {
      return await rsApiCall('/rsAccounts/importIdentity', params: mParams);
    } catch (e) {
      if (e.toString().contains('404')) {
        // Try fallback path
        return await rsApiCall('/rsLoginHelper/importPGPIdentity', params: mParams);
      }
      rethrow;
    }
  }

  static Future<dynamic> getPGPDetails(String gpgId) async {
    final mParams = {
      'gpg_id': gpgId,
    };
    const mPath = '/rsAccounts/getPGPDetails';
    return await rsApiCall(mPath, params: mParams);
  }
}

class RsLoginHelper {
  /// Creates a Retroshare Account
  /// Returns the location to use the account from now on

  /// Login in function
  ///
  /// You can send custom [location] and [password] to configure the global ones
  /// defined in [authLocationId] and [authPassphrase].
  ///
  /// If is already logged in it execute [RsIdentity.getOrCreateIdentity].
  ///
  /// Else do an `/rsLoginHelper/attemptLogin`.
  ///
  /// Return 0 if everything is Ok
  static Future<dynamic> requestLogIn(
    dynamic selectedAccount,
    String password, [
    String? apiUser,
    String? apiPass,
  ]) async {
    final accountDetails = {
      'account': selectedAccount.locationId,
      'password': password,
      if (apiUser != null) 'apiUser': apiUser,
      if (apiPass != null) 'apiPass': apiPass,
    };
    const mPath = '/rsLoginHelper/attemptLogin';
    final response = await rsApiCall(mPath, params: accountDetails);
    print(response);
    return response['retval'];
  }

  /// Check if is logged in
  ///
  /// Return false if `/rsLoginHelper/isLoggedIn` is false and also if one of
  /// the variables [authLocationId] or [authPassphrase] is null, (what mean
  /// that are not set).
  static Future<dynamic> checkLoggedIn() async {
    const mPath = '/rsLoginHelper/isLoggedIn';
    final response = await rsApiCall(mPath);
    final retval = response['retval'];
    if (retval is bool) return retval;
    if (retval is int) return retval == 1;
    return false;
  }

  static Future<dynamic> getLocations() async {
    try {
      const mPath = '/rsLoginHelper/getLocations';
      final response = await rsApiCall(mPath);
      return response['locations'];
    } catch (e) {
      return [];
    }
  }

  static Future<dynamic> requestAccountCreation(
    String username,
    String password, [
    String nodeName = 'mobile',
    String? apiUser,
    String? apiPass,
  ]) async {
    final mParams = {
      'locationName': nodeName,
      'pgpName': username,
      'password': password,
      'apiUser': apiUser ?? username,
      'apiPass': apiPass ?? password,
    };
    const mPath = '/rsLoginHelper/createLocationV2';
    final response = await rsApiCall(mPath, params: mParams);
    return response;
  }

  static Future<dynamic> importLocation(String base64Cert, String password) async {
    final mParams = {
      'base64Cert': base64Cert,
      'password': password,
    };
    const mPath = '/rsLoginHelper/importLocation';
    final response = await rsApiCall(mPath, params: mParams);
    return response;
  }

  static Future<dynamic> createLocation(String gpgId, String password, [String nodeName = 'mobile']) async {
    final mParams = {
      'gpgId': gpgId,
      'locationName': nodeName,
      'password': password,
    };
    const mPath = '/rsLoginHelper/createLocation';
    final response = await rsApiCall(mPath, params: mParams);
    return response;
  }
}

class RsIdentity {
  /// Create a new identity
  ///
  /// Return [id] storage for the created identity Id
  /// [name] Name of the identity
  /// NOT SUPORTED avatar Image associated to the identity
  /// NOT SUPORTED (default true) [pseudonimous] true for unsigned identity, false otherwise
  /// [pgpPassword] password to unlock PGP to sign identity, not implemented yet
  /// @return false on error, true otherwise

  // Returns the ID of the default identity or creates one if there is none defined
  static Future<dynamic> getOwnSignedIdentity(AuthToken authToken) async {
    try {
      const mPath = '/rsIdentity/getOwnSignedIds';
      final respSigned = await rsApiCall(mPath, authToken: authToken);
      return respSigned['ids'];
    } catch (e) {
      return [];
    }
  }

  static Future<dynamic> getOwnPseudonimousIds(AuthToken authToken) async {
    try {
      const mPath = '/rsIdentity/getOwnPseudonimousIds';
      final respSigned = await rsApiCall(mPath, authToken: authToken);
      return respSigned['ids'];
    } catch (e) {
      return [];
    }
  }

  static Future<bool> isKnownId(String? sslId, AuthToken? authToken) async {
    try {
      const mPath = '/rsIdentity/isKnownId';
      final mParams = {'id': sslId};
      final response =
          await rsApiCall(mPath, authToken: authToken, params: mParams);

      final retval = response['retval'];
      return (retval is bool && retval) || (retval is int && retval == 1);
    } catch (err) {
      throw Exception('something went wrong!');
    }
  }

  static Future<void> requestIdentity(String sslId, AuthToken authToken) async {
    try {
      const mPath = '/rsIdentity/requestIdentity';
      final mParams = {'id': sslId};
      final response =
          await rsApiCall(mPath, authToken: authToken, params: mParams);

      final retval = response['retval'];
      final bool success = (retval is bool && retval) || (retval is int && retval == 1);
      if (!success) throw Exception();
    } catch (err) {
      rethrow;
    }
  }

  static Future<Map> getIdDetails(
    String identityId,
    AuthToken authToken,
  ) async {
    const mPath = '/rsIdentity/getIdDetails';
    final mParams = {'id': identityId};
    var response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);

    var retval = response['retval'];
    var success = (retval is bool && retval) || (retval is int && retval == 1);

    if (!success) {
      // TODO(nicoechaniz): for some reason RS is not getting this right all the time and response["details"] responds with
      // a json with the structure byt no data, so we repeat with a delay. Check upstream if this is intended.
      await Future.delayed(const Duration(seconds: 2));
      response = await rsApiCall(mPath, authToken: authToken, params: mParams);
      retval = response['retval'];
      success = (retval is bool && retval) || (retval is int && retval == 1);
    }
    if (!success) {
      throw Exception('Could not retrieve details for id $identityId');
    }
    return response;
  }

  ///  Get identities summaries list.
  ///
  /// Return  [ids] list where to store the identities
  static Future<List<dynamic>> getIdentitiesSummaries(
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsIdentity/getIdentitiesSummaries',
      authToken: authToken,
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve identities summaries');
    }
    return response['ids'] ?? [];
  }

  /// Get identities information (name, avatar...).
  ///
  /// [ids] ids of the channels of which to get the informations
  /// return [idsInfo] storage for the identities informations
  static Future<List<dynamic>> getIdentitiesInfo(
    List<String> ids,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsIdentity/getIdentitiesInfo',
      authToken: authToken,
      params: {'ids': ids},
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception("Can't retrieve getIdentitiesInfo for $ids");
    }
    return response['idsInfo'] ?? [];
  }

  static Future<Identity> createIdentity(
    Identity identity,
    RsGxsImage avatar,
    AuthToken authToken, [
    String? pgpPassword,
  ]) async {
    try {
      final params = {
        'name': identity.name ?? '',
        'pseudonimous': !identity.signed,
        'pgpPassword': pgpPassword ?? authToken.password,
      };
      params['avatar'] = avatar.toJson();
      const mPath = '/rsIdentity/createIdentity';

      final response =
          await rsApiCall(mPath, authToken: authToken, params: params);
      final retval = response['retval'];
      if ((retval is bool && retval) || (retval is int && retval == 1)) {
        return Identity(
          mId: response['id'],
          signed: identity.signed,
          name: identity.name,
          avatar: avatar.base64String,
          isContact: false,
        );
      }
      return const Identity(mId: '', signed: false, isContact: false);
    } catch (e) {
      throw Exception('Failed to load response');
    }
  }

  static Future<bool?> addFriend(
    String sslId,
    String gpgId,
    AuthToken authToken,
  ) async {
    try {
      const mPath = '/rsPeers/addFriend';
      final mParams = {'sslId': sslId, 'gpgId': gpgId};

      await rsApiCall(mPath, authToken: authToken, params: mParams);
    } catch (e) {
      throw Exception('Failed to load response');
    }
    return null;
  }

  static Future<bool> setAutoAddFriendIdsAsContact(
    bool enabled,
    AuthToken authToken,
  ) async {
    const mPath = '/rsIdentity/setAutoAddFriendIdsAsContact';

    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: {'enabled': enabled},
    );

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Update identity data (name, avatar...)
  ///
  /// id Id of the identity
  /// name New name of the identity
  /// Optional [avatar] New avatar for the identity in RsGxsImage format.
  ///
  /// NOT SUPORTED [pseudonimous] Set to true to make the identity anonymous.
  /// If set to false, updating will require the profile passphrase.
  /// [pgpPassword] Profile passphrase, if non pseudonymous.
  /// @return false on error, true otherwise
  static Future<bool> updateIdentity(
    Identity identity,
    RsGxsImage avatar,
    AuthToken authToken, [
    String? pgpPassword,
  ]) async {
    final params = {
      'name': identity.name,
      'id': identity.mId,
      'pseudonimous': !identity.signed,
      'pgpPassword': pgpPassword ?? authToken.password,
    };
    if (identity.avatar != null) params['avatar'] = avatar.toJson();
    const mPath = '/rsIdentity/updateIdentity';

    final response =
        await rsApiCall(mPath, authToken: authToken, params: params);

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Set/unset identity as contact
  ///
  /// @param[in] [id] Id of the identity
  /// @param[in] [isContact] true to set, false to unset
  /// @return false on error, true otherwise
  static Future<bool> setAsRegularContact(
    String id,
    bool isContact,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsIdentity/setAsRegularContact',
      authToken: authToken,
      params: {'id': id, 'isContact': isContact},
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> setContact(
    String id,
    bool makeContact,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsIdentity/setAsRegularContact',
      authToken: authToken,
      params: {'id': id, 'isContact': makeContact},
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> deleteIdentity(
    Identity identity,
    AuthToken authToken,
  ) async {
    const mPath = '/rsIdentity/deleteIdentity';
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: {'id': identity.mId},
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    return !success; // Based on original logic
  }
}

// ----------------------------------------------------------------------------
// Join the network
// ----------------------------------------------------------------------------

class RsPeers {
  /// get the certificate/invite for current identity
  static Future<String> getOwnCert(AuthToken authToken) async {
    const mPath = '/rsPeers/GetRetroshareInvite';

    final response = await rsApiCall(mPath, authToken: authToken);

    return response['retval'];
  }

  /// Get RetroShare short invite of the given peer
  ///
  /// [sslId] Id of the peer of which we want to generate an invite,
  ///	a null id (all 0) is passed, an invite for own node is returned.
  /// NOT SUPPORTED [inviteFlags] specify extra data to include in the invite and
  ///	format.
  /// [baseUrl] URL into which to sneak in the RetroShare invite
  ///	radix, this is primarly useful to trick other applications into making
  ///	the invite clickable, or to disguise the RetroShare invite into a
  ///	"normal" looking web link. Used only if formatRadix is false.
  /// @return false if error occurred, true otherwise
  /// @param[out] invite storage for the generated invite
  static Future<String> getShortInvite(
    AuthToken authToken, {
    String? sslId,
    String? baseUrl,
  }) async {
    final mParams = {
      'sslId': sslId,
    };
    if (baseUrl != null) mParams['baseUrl'] = baseUrl;
    final response = await rsApiCall(
      '/rsPeers/GetShortInvite',
      authToken: authToken,
      params: mParams,
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not get short invite for $sslId');
    }
    final inviteStr = response['invite'] as String? ?? '';
    try {
      final inviteUri = Uri.parse(inviteStr);
      final shortKey = inviteUri.queryParameters['rsInvite'];
      if (shortKey != null) {
        return Uri.decodeComponent(shortKey);
      }
    } catch (_) {}
    return Uri.decodeComponent(inviteStr.substring(31));
  }

  static Future<bool> addSslOnlyFriend(
    AuthToken authToken,
    String sslId,
    String pgpId,
    Map details,
  ) async {
    const mPath = '/rsPeers/addSslOnlyFriend';
    final mParams = {'sslId': sslId, 'pgpId': pgpId, 'details': details};
    final response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> isSslOnlyFriend(String sslId, AuthToken authToken) async {
    const mPath = '/rsPeers/isSslOnlyFriend';
    final mParams = {'sslId': sslId};
    final response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Accepts long invite codes only
  static Future<bool> acceptInvite(
    AuthToken authToken,
    String base64Payload, {
    http.Client? client,
  }) async {
    const mPath = '/rsPeers/acceptInvite';
    final mParams = {'invite': base64Payload};
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: mParams,
      client: client,
    );
    print('API Response (acceptInvite): $response');
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Accepts short invite codes only
  static Future<bool> acceptShortInvite(
    AuthToken authToken,
    String shortBase64Payload,
  ) async {
    final details =
        await RsPeers.parseShortInvite(authToken, shortBase64Payload);

    const mPath = '/rsPeers/addSslOnlyFriend';
    final params = {
      'sslId': details['id'],
      'pgpId': details['gpg_id'],
      'details': details,
    };
    final response =
        await rsApiCall(mPath, authToken: authToken, params: params);

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<void> connectAttempt(String sslId, AuthToken authToken) async {
    const mPath = '/rsPeers/connectAttempt';
    final params = {'sslId': sslId};
    final response =
        await rsApiCall(mPath, authToken: authToken, params: params);

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('The connection attempt could not be completed');
    }
  }

  static Future<Map<String, dynamic>> parseShortInvite(
    AuthToken authToken,
    String shortBase64Payload,
  ) async {
    const mPath = '/rsPeers/parseShortInvite';
    final params1 = {'invite': shortBase64Payload};
    final response =
        await rsApiCall(mPath, authToken: authToken, params: params1);

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    
    if (!success) {
      throw Exception('Could not parse the short invite code');
    } else if (response['details'] is! Map ||
        response['details']['id'] is! String ||
        response['details']['gpg_id'] is! String) {
      throw Exception('Could not parse the short invite code');
    }

    return response['details'];
  }

  static Future<Location> getPeerFriendDetails(
    String sslId,
    AuthToken authToken, {
    http.Client? client,
  } // Add client
      ) async {
    // This function calls getPeerDetails and potentially others.
    // Refactoring it requires refactoring the methods it calls first.
    // We'll stick to getPeerDetails for now as the primary call.
    return getPeerDetails(sslId, authToken, client: client);
    // The isSslOnlyFriend/addSslOnlyFriend logic might need separate testing/
    // handling if those methods also need mocking.
  }

  static Future<Location> getPeerDetails(
    String sslId,
    AuthToken authToken, {
    http.Client? client,
  } // Add client
      ) async {
    const mPath = '/rsPeers/getPeerDetails';
    final mParams = {'sslId': sslId};

    final response = await rsApiCall(
      mPath, // Pass client
      authToken: authToken,
      params: mParams,
      client: client,
    );

    // rsApiCall checks retval=true, so we only need to check the structure.
    if (response['det'] is! Map) {
      // Update exception message to match test
      throw Exception('API Error: Peer details not found (Invalid structure)');
    }
      final presence = response['det']['mPresence'] ?? response['det']['presence'] ?? response['det']['mPresenceData'];
      return Location(
        rsPeerId: response['det']['id'],
        rsGpgId: response['det']['gpg_id'],
        accountName: response['det']['name'],
        locationName: response['det']['location'],
        isOnline: response['det']['connectState'] != 0 &&
            response['det']['connectState'] != 2 &&
            response['det']['connectState'] != 3,
        status: presence != null ? (presence['mStatus'] ?? presence['status'] ?? presence['mPresenceStatus'] ?? 0) : 0,
      );
  }

  static Future<List<String>> getOnlineList(
    AuthToken authToken, {
    http.Client? client,
  }) async {
    // Add client
    const mPath = '/rsPeers/getOnlineList';
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      client: client,
    ); // Pass client

    if (response['sslIds'] is! List) {
      // Update exception message to match test
      throw Exception('API Error: Failed to get list (Invalid format)');
    }

    return response['sslIds'].cast<String>().toList();
  }

  static Future<List<String>> getFriendList(
    AuthToken authToken, {
    http.Client? client,
  }) async {
    // Add client
    const mPath = '/rsPeers/getFriendList';
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      client: client,
    );
    print('API Response (getFriendList): $response');

    if (response['sslIds'] is List) {
      return response['sslIds'].cast<String>().toList();
    }
    
    // Some versions return empty list if no friends, but ensure we return something valid
    return [];
  }

  static Future<List<dynamic>> getGroupInfoList(AuthToken authToken) async {
    final response =
        await rsApiCall('/rsPeers/getGroupInfoList', authToken: authToken);
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve groups info');
    }
    return response['groupInfoList'];
  }

  static Future<bool> isOnline(
    String sslId,
    AuthToken authToken, {
    http.Client? client,
  }) async {
    const mPath = '/rsPeers/isOnline';
    final params = {'sslId': sslId};
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: params,
      client: client,
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> isFriend(String sslId, AuthToken authToken) async {
    const mPath = '/rsPeers/isFriend';
    final params = {'sslId': sslId};
    final response =
        await rsApiCall(mPath, authToken: authToken, params: params);

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> removeFriend(
    String pgpId,
    AuthToken authToken, {
    http.Client? client,
  }) async {
    const mPath = '/rsPeers/removeFriend';
    final params = {'pgpId': pgpId};
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: params,
      client: client,
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception(
        'Remove friend API call failed: ${response["errorMessage"] ?? "retval was not true"}',
      );
    }
    return true;
  }
}

// ----------------------------------------------------------------------------
// Peer Status (online, away, busy, idle)
// ----------------------------------------------------------------------------

class RsStatus {
  /// Get status list for all connected peers.
  ///
  /// Returns a map of peerId -> status value:
  ///   0x0000 = RS_STATUS_OFFLINE
  ///   0x0001 = RS_STATUS_AWAY
  ///   0x0002 = RS_STATUS_BUSY
  ///   0x0003 = RS_STATUS_ONLINE
  ///   0x0004 = RS_STATUS_INACTIVE
  static Future<Map<String, int>> getStatusList(
    AuthToken authToken, {
    http.Client? client,
  }) async {
    final statusMap = <String, int>{};
    try {
      final response = await rsApiCall(
        '/rsStatus/getStatusList',
        authToken: authToken,
        client: client,
      );

      // RetroShare uses C++ parameter names: statusInfo (camelCase)
      final statusList = response['statusInfo'] ?? response['status_info'];
      if (statusList is List) {
        for (final info in statusList) {
          if (info is Map) {
            final id = info['id'] ?? info['mPeerId'] ?? '';
            final status = (info['status'] as num?)?.toInt() ??
                (info['mStatus'] as num?)?.toInt() ??
                0;
            if (id is String && id.isNotEmpty) {
              statusMap[id] = status;
            }
          }
        }
      }
    } catch (e) {
      // Status API may not be available; silently fall back to defaults
    }
    return statusMap;
  }
}

// ----------------------------------------------------------------------------
// Broadcast Discovery
// ----------------------------------------------------------------------------

class RsBroadcastDiscovery {
  static Future<void> enableMulticastListening() async {
    final response =
        await rsApiCall('/rsBroadcastDiscovery/isMulticastListeningEnabled');
    final retval = response['retval'];
    final bool isEnabled = (retval is bool && retval) || (retval is int && retval == 1);
    if (!isEnabled) {
      await rsApiCall('/rsBroadcastDiscovery/enableMulticastListening');
    }
  }

  static Future<List> getDiscoveredPeers() async {
    var response;
    try {
      response = await rsApiCall('/rsBroadcastDiscovery/getDiscoveredPeers');
    } catch (error) {
      throw Exception('Error discovering peers. $error');
    }
    return response['retval'];
  }
}

// ----------------------------------------------------------------------------
// Individual messaging
// ----------------------------------------------------------------------------

class RsMsgs {
  /// Sends a private message (payload) to the node(s) from the list and returns
  /// an array with the delivery ID
  static Future<List<String>> sendMail(
    String id,
    List<String> to,
    Map<String, dynamic> payload,
  ) async {
    const mPath = '/rsMail/sendMail';
    final mParams = {'from': id, 'to': to, 'mailBody': jsonEncode(payload)};
    final response = await rsApiCall(mPath, params: mParams);

    if (response['errorMsg'] is String && response['errorMsg'].length > 0) {
      throw Exception(response['errorMsg']);
    } else if (response['retval'] < to.length) {
      throw Exception('The message could not be delivered to all recipients');
    }

    if (response['trackingIds'] is! List) {
      throw Exception('The message could not be delivered');
    }

    final trackingIds = <String>[];
    for (final item in response['trackingIds'] ?? []) {
      if (item['mMailId'] is String) trackingIds.add(item['mMailId']);
    }

    return trackingIds;
  }

  /// Returns a list of {msgId, srcId, msgflags, msgtags}
  static Future<List<Map<String, dynamic>>> getMessageSummaries() async {
    final response = await rsApiCall('/rsMail/getMessageSummaries');

    if (response['msgList'] is! List) {
      throw Exception('Could not retrieve the message summaries');
    }
    return response['msgList'].cast<Map<String, dynamic>>().toList() ?? [];
  }

  static Future<Map<String, dynamic>?> getMessage(String msgId) async {
    if (msgId.isEmpty) {
      throw Exception('Invalid msgId');
    }

    final response =
        await rsApiCall('/rsMail/getMessage', params: {'msgId': msgId});

    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) return null;
    return response['msg'] ?? {};
  }

  static Future<bool> messageDelete(String msgId) async {
    if (msgId.isEmpty) {
      throw Exception('Invalid msgId');
    }

    final response =
        await rsApiCall('/rsMail/MessageDelete', params: {'msgId': msgId});

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<bool> createChatLobby(
    AuthToken authToken,
    String lobbyName,
    String idToUse,
    String lobbyTopic, {
    List<dynamic> inviteList = const [],
    bool public = true,
    bool anonymous = true,
  }) async {
    // Determine privacy type first
    var privacyType = 0;
    if (public && anonymous) {
      privacyType = 4;
    } else if (public && !anonymous) {
      privacyType = 20;
    } else if (!public && !anonymous) {
      privacyType = 16;
    }

    // Create ReqCreateChatLobby using the constructor with named parameters
    final req = ReqCreateChatLobby(
      lobbyName: lobbyName,
      lobbyTopic: lobbyTopic,
      lobbyIdentity: idToUse,
      invitedFriends: inviteList.isNotEmpty
          ? List<String>.from(inviteList.map((e) {
              if (e is String) return e;
              try {
                // If it's a Location object, we need the peer ID
                return (e as dynamic).rsPeerId.toString();
              } catch (_) {
                return e.toString();
              }
            }))
          : [], // Use empty list instead of null
      lobbyPrivacyType: privacyType,
    );

    final response = await rsApiCall(
      '/rsChats/createChatLobby',
      authToken: authToken,
      params: req.toJson(),
    );
    
    final rv = response['retval'];
    bool isSuccess = false;
    String? newLobbyId;

    if (rv is Map) {
      if (rv['xstr64'] != null && rv['xstr64'] != '0' && rv['xstr64'] != '') {
        isSuccess = true;
        newLobbyId = rv['xstr64'];
      } else if (rv['xint64'] != null && rv['xint64'] != 0) {
        isSuccess = true;
        newLobbyId = rv['xint64'].toString();
      }
    }

    if (isSuccess && newLobbyId != null) {
      await setLobbyAutoSubscribe(
        newLobbyId,
        authToken,
      );
      return true;
    }
    throw Exception('Failed to load response');
  }

  static Future<void> setLobbyAutoSubscribe(
    String lobbyId,
    AuthToken authToken, [
    bool subs = true,
  ]) async {
    // Create ChatLobbyId using the constructor
    final chatLobbyId = ChatLobbyId(xstr64: lobbyId);
    final params = {'lobby_id': chatLobbyId.toJson(), 'autoSubscribe': subs};

    await rsApiCall(
      '/rsChats/setLobbyAutoSubscribe',
      authToken: authToken,
      params: params,
    );
  }

  static Future<bool> getLobbyAutoSubscribe(
    String lobbyId,
    AuthToken authToken, [
    bool subs = true,
  ]) async {
    // Create ChatLobbyId using the constructor
    final chatLobbyId = ChatLobbyId(xstr64: lobbyId);
    final params = {'lobby_id': chatLobbyId.toJson()};

    final response = await rsApiCall(
      '/rsChats/getLobbyAutoSubscribe',
      authToken: authToken,
      params: params,
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<void> unsubscribeChatLobby(
    String lobbyId,
    AuthToken authToken,
  ) async {
    // Create ChatLobbyId using the constructor
    final chatLobbyId = ChatLobbyId(xstr64: lobbyId);
    final params = {
      'lobby_id': chatLobbyId.toJson(),
    };

    await rsApiCall(
      '/rsChats/unsubscribeChatLobby',
      authToken: authToken,
      params: params,
    );
  }

  static Future<bool> sendMessage(
    String chatId,
    String msgTxt,
    AuthToken authToken, [
    ChatIdType type = ChatIdType.type2,
  ]) async {
    // Create ChatId using the constructor with named parameters
    ChatId id;
    // Correct the enum name checks
    if (type == ChatIdType.type2) {
      id = ChatId(type: type, distantChatId: chatId);
    } else if (type == ChatIdType.type3) {
      // Create ChatLobbyId using the constructor
      id = ChatId(type: type, lobbyId: ChatLobbyId(xstr64: chatId));
    } else {
      throw 'Chat type not supported';
    }

    final params = {'id': id.toJson(), 'msg': msgTxt};
    print('DEBUG: RsMsgs.sendMessage params: ${jsonEncode(params)}');
    final response = await rsApiCall(
      '/rsChats/sendChat',
      authToken: authToken,
      params: params,
    );
    print('DEBUG: RsMsgs.sendMessage response: $response');
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<void> denyLobbyInvite(
    String lobbyId,
    AuthToken authToken,
  ) async {
    await rsApiCall(
      '/rsChats/denyLobbyInvite',
      authToken: authToken,
      params: {
        'id': {'xstr64': lobbyId},
      },
    );
  }

  static Future<bool> acceptLobbyInvite(
    String lobbyId,
    String rsgxsId,
    AuthToken authToken,
  ) async {
    const mPath = '/rsChats/acceptLobbyInvite';
    final response = await rsApiCall(
      mPath,
      authToken: authToken,
      params: {
        'id': {'xstr64': lobbyId},
        'identity': rsgxsId,
      },
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<Map<dynamic, dynamic>> c(Chat chat, AuthToken authToken) async {
    final params = {
      'to_pid': chat.interlocutorId,
      'from_pid': chat.ownIdToUse,
      'notify': true,
    };
    final response = await rsApiCall(
      '/rsChats/initiateDistantChatConnexion',
      authToken: authToken,
      params: params,
    );
    return response;
  }

  /// Get the chat status from [pid]
  ///  #define RS_DISTANT_CHAT_STATUS_UNKNOWN			0x0000
  ///  #define RS_DISTANT_CHAT_STATUS_TUNNEL_DN   		0x0001
  ///  #define RS_DISTANT_CHAT_STATUS_CAN_TALK			0x0002
  ///  #define RS_DISTANT_CHAT_STATUS_REMOTELY_CLOSED 	0x0003
  static Future<DistantChatPeerInfo> getDistantChatStatus(
    AuthToken authToken,
    String pid,
    ChatMessage aaa,
  ) async {
    final response = await rsApiCall(
      '/rsChats/getDistantChatStatus',
      authToken: authToken,
      params: {'pid': pid},
    );
    // Note: On status 3 (remotely closed), retval might be true or false depending on core version
    // but the 'info' field usually contains the status.
    if (response['info'] != null) {
      return DistantChatPeerInfo.fromJson(response['info']);
    }

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw 'Error on getDistantChatStatus()';
    }
    return DistantChatPeerInfo.fromJson(response['info']);
  }

  static Future<List<VisibleChatLobbyRecord>> getUnsubscribedChatLobbies(
    AuthToken authToken,
  ) async {
    final unsubscribedChatLobby = <VisibleChatLobbyRecord>[];
    final chatLobbies = await rsApiCall(
      '/rsChats/getListOfNearbyChatLobbies',
      authToken: authToken,
    );
    for (final visible in chatLobbies['public_lobbies']) {
      final chat = VisibleChatLobbyRecord.fromJson(visible);
      /*await getLobbyAutoSubscribe(chat.lobbyId.xstr64, authToken) ?? true;*/
      unsubscribedChatLobby.add(chat);
    }
    return unsubscribedChatLobby;
  }

  static Future<bool> joinChatLobby(
    String chatId,
    String idToUse,
    AuthToken authToken,
  ) async {
    const mPath = '/rsChats/joinVisibleChatLobby';
    final mParams = {
      'lobby_id': {'xstr64': chatId},
      'own_id': idToUse,
    };

    final response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (success) await setLobbyAutoSubscribe(chatId, authToken);
    return success;
  }

  static Future<bool> closeDistantChatConnexion(
    String chatId,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsChats/closeDistantChatConnexion',
      authToken: authToken,
      params: {'pid': chatId},
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<dynamic> getChatLobbyInfo(
    String lobbyId,
    AuthToken authToken,
  ) async {
    const mPath = '/rsChats/getChatLobbyInfo';
    final mParams = {
      'id': {'xstr64': lobbyId},
    };
    final response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);
    final retval = response['retval'];
    if ((retval is bool && retval) || (retval is int && retval == 1)) {
      return response['info'];
    } else {
      throw Exception('Something went wrong!');
    }
  }

  static Future<dynamic> getPendingChatLobbyInvites(AuthToken authToken) async {
    const mPath = '/rsChats/getPendingChatLobbyInvites';
    final response = await rsApiCall(mPath, authToken: authToken);
    print(response);
    return response['invites'];
  }

  static Future<dynamic> getSubscribedChatLobbies(AuthToken authToken) async {
    const mPath = '/rsChats/getChatLobbyList';
    final response = await rsApiCall(mPath, authToken: authToken);
    return response['cl_list'];
  }

  static Future<dynamic> getLobbyParticipants(
    String lobbyId,
    AuthToken authToken,
  ) async {
    final mParams = {
      'id': {'xstr64': lobbyId},
    };
    const mPath = '/rsChats/getChatLobbyInfo';
    final response =
        await rsApiCall(mPath, authToken: authToken, params: mParams);
    return response['info']['gxs_ids'];
  }

}

// ----------------------------------------------------------------------------
// Chat service
// ----------------------------------------------------------------------------

class RsChats {
  static Future<String> getOwnCustomStateString(AuthToken authToken) async {
    final response = await rsApiCall(
      '/rsChats/getOwnCustomStateString',
      authToken: authToken,
    );
    return response['retval']?.toString() ?? '';
  }

  static Future<String> getCustomStateString(
    String peerId,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsChats/getCustomStateString',
      authToken: authToken,
      params: {'peer_id': peerId},
    );
    return response['retval']?.toString() ?? '';
  }

  static Future<void> setCustomStateString(
    String statusString,
    AuthToken authToken,
  ) async {
    await rsApiCall(
      '/rsChats/setCustomStateString',
      authToken: authToken,
      params: {'status_string': statusString},
    );
  }
}

// ----------------------------------------------------------------------------
// Messages broadcasted to channels
// ----------------------------------------------------------------------------

class RsGxsChannel {
  static Future<void> subscribe(String channelId) async {
    final response = await rsApiCall(
      '/rsGxsChannels/subscribeToChannel',
      params: {'channelId': channelId, 'subscribe': true},
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not subscribe');
    }
  }

  static Future<void> unsubscribe(String channelId) async {
    final response = await rsApiCall(
      '/rsGxsChannels/subscribeToChannel',
      params: {'channelId': channelId, 'subscribe': false},
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not unsubscribe');
    }
  }

  /// Fetch the list of all contents, along with their ID, status and timestamp.
  /// Returns a map like { mMsgId: "", mmOrigMsgId: "", mMsgFlags: 0x1234, mMsgStatus: 0x1234 }
  static Future<List<Map<String, dynamic>>> getContentSummaries(
    String channelId,
  ) async {
    if (channelId.isEmpty) {
      throw Exception('Invalid channel ID');
    }

    final response = await rsApiCall(
      '/rsGxsChannels/getContentSummaries',
      params: {'channelId': channelId},
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not subscribe');
    } else if (response['summaries'] is! List) {
      throw Exception('Invalid summaries');
    }

    return (response['summaries'] as List)
        .cast<Map<String, dynamic>>()
        .toList();
  }

  static Future<List<Map<String, dynamic>>> getChannelsSummaries() async {
    final response = await rsApiCall('/rsGxsChannels/getChannelsSummaries');

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve the summary');
    } else if (response['channels'] is! List) {
      throw Exception('Invalid channels');
    }

    return (response['channels'] as List).cast<Map<String, dynamic>>().toList();
  }

  /// Fetches the given messages from the Channel
  static Future<List<Map<String, dynamic>>> getChannelItems(
    String channelId,
    List<String> msgIds,
  ) async {
    if (channelId.isEmpty) {
      throw Exception('Invalid channel ID');
    }

    final response = await rsApiCall(
      '/rsGxsChannels/getChannelContent',
      params: {'channelId': channelId, 'contentsIds': msgIds},
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not subscribe');
    } else if (response['posts'] is! List) throw Exception('Invalid posts');

    return (response['posts'] as List).cast<Map<String, dynamic>>().toList();
  }

  /// Fetches the relevant messages from the Channel
  static Future<List<Map<String, dynamic>>> getChannelContent(
    String channelId,
  ) async {
    final summaries = await getContentSummaries(channelId);
    final msgIds = summaries
        .map((item) => item['mMsgId'] ?? '')
        .toList()
        .cast<String>()
        .asMap();

    if (channelId.isEmpty) {
      throw Exception('Invalid channel ID');
    }

    // TODO: filter by relevant stuff only

    final response = await rsApiCall(
      '/rsGxsChannels/getChannelContent',
      params: {'channelId': channelId, 'contentsIds': msgIds},
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not subscribe');
    } else if (response['posts'] is! List) {
      throw Exception('Invalid posts');
    }

    return (response['posts'] as List).cast<Map<String, dynamic>>().toList();
  }

  /// Sends a public message to all the channel, on the given thread ID.
  /// Returns the commentMessageId
  static Future<String> createComment(
    String channelId,
    String threadId,
    String identityId,
    Map<String, dynamic> payload,
  ) async {
    if (channelId.isEmpty) {
      throw Exception('Invalid channel ID');
    } else if (threadId.isEmpty) {
      throw Exception('Invalid thread ID');
    }

    final response = await rsApiCall(
      '/rsGxsChannels/createCommentV2',
      params: {
        'authorId': identityId,
        'channelId': channelId,
        'threadId': threadId,
        'comment': payload,
        'parentId': threadId,
        // "origCommentId": "",
      },
    );

    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not create the comment');
    } else if (response['errorMessage'] is String &&
        response['errorMessage'].length > 0) {
      throw Exception(response['errorMessage']);
    } else if (response['commentMessageId'] is! String) {
      throw Exception('Invalid commentMessageId');
    }

    return response['commentMessageId'];
  }
}

// ----------------------------------------------------------------------------
// Forums
// ----------------------------------------------------------------------------

class RsGxsForum {
  static Future<bool> toggleSubscribeToForum(
    String forumId,
    bool subscribe,
    AuthToken authToken,
  ) async {
    final response = await rsApiCall(
      '/rsGxsForums/subscribeToForum',
      authToken: authToken,
      params: {'forumId': forumId},
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<String> createForumV2(
    String name, {
    String circleId = '',
  }) async {
    final circleType =
        circleId.isEmpty ? RsGxsCircleType.PUBLIC : RsGxsCircleType.EXTERNAL;

    final response = await rsApiCall(
      '/rsGxsForums/createForumV2',
      params: {
        'name': name,
        'circleType': circleType.index,
        'circleId': circleId,
      },
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Forum could not be created.');
    }
    return response['forumId'];
  }

  static Future<String> createPost(
    String forumId,
    String title,
    String mBody,
    String authorId, [
    String parentId = '',
    String origPostId = '',
  ]) async {
    final response = await rsApiCall(
      '/rsGxsForums/createPost',
      params: {
        'forumId': forumId,
        'title': title,
        'mBody': mBody,
        'authorId': authorId,
        'parentId': parentId,
        'origPostId': origPostId,
      },
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('${response["errorMessage"]}');
    }
    return response['postMsgId'];
  }

  static Future<List<dynamic>> getForumsInfo(List<String> forumIds) async {
    final response = await rsApiCall(
      '/rsGxsForums/getForumsInfo',
      params: {'forumIds': forumIds},
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve forums info');
    }
    return response['forumsInfo'];
  }

  static Future<List<dynamic>> getForumsSummaries() async {
    final response = await rsApiCall('/rsGxsForums/getForumsSummaries');
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve forum summaries');
    }
    return response['forums'];
  }

  static Future<List<RsMsgMetaData>> getForumMsgMetaData(String forumId) async {
    final response = await rsApiCall(
      '/rsGxsForums/getForumMsgMetaData',
      params: {'forumId': forumId},
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve messages metadata');
    }
    return [
      for (final Map<String, dynamic> meta in response['msgMetas'])
        RsMsgMetaData.fromJson(meta),
    ];
  }

  static Future<List<dynamic>> getForumContent(
    String forumId,
    List<String> msgIds,
  ) async {
    final response = await rsApiCall(
      '/rsGxsForums/getForumContent',
      params: {'forumId': forumId, 'msgsIds': msgIds},
    );
    final retval = response['retval'];
    final bool success = (retval is bool && retval) || (retval is int && retval == 1);
    if (!success) {
      throw Exception('Could not retrieve messages content');
    }
    return response['msgs'];
  }

  static Future<bool> subscribeToForum(String forumId, bool subscribe) async {
    final response = await rsApiCall(
      '/rsGxsForums/subscribeToForum',
      params: {'forumId': forumId, 'subscribe': true},
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<void> requestSynchronization() async {
    try {
      await rsApiCall('/rsGxsForums/requestSynchronization');
    } catch (err) {
      print('/rsGxsForums/requestSynchronization not available $err');
    }
  }

  static Future<List<dynamic>> getChildPosts(
    String forumId,
    String parentId,
  ) async {
    final response = await rsApiCall(
      '/rsGxsForums/getChildPosts',
      params: {'forumId': forumId, 'parentId': parentId},
    );
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception(
        'Could not retrieve child posts for $forumId/$parentId. Response: $response',
      );
    }
    final List childPosts = response['childPosts'];
    // sort last comment on top
    childPosts.sort((a, b) => publishTs(b).compareTo(publishTs(a)));

//    final postsMeta =
//        childPosts.map((item) => item["mMeta"]).toList();
    return childPosts;
  }

  static Future<int> distantSearchRequest(String matchString) async {
    dbg('Starting distant search for: $matchString');

    final response = await rsApiCall(
      '/rsGxsForums/distantSearchRequest',
      params: {'matchString': matchString},
    );
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception("Error: ${errToStr(response["retval"])}");
    }
    return response['searchId'];
  }

  static Future<List<dynamic>> localSearch(String matchString) async {
    dbg('Executing local search for: $matchString');

    final response = await rsApiCall(
      '/rsGxsForums/localSearch',
      params: {'matchString': matchString},
    );
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception("Error: ${errToStr(response["retval"])}");
    }

    dbg('Results found:');
    dbg(response['searchResults'].toString());
    return response['searchResults'];
  }
}

String publishTs(Map post) {
  final String pts = post['mMeta']['mPublishTs']['xstr64'];
  return pts;
}
// ----------------------------------------------------------------------------
// Files
// ----------------------------------------------------------------------------

class RsFiles {
// period defaults to 10 years
  static Future<bool> extraFileHash(
    String localPath, [
    int period = 31536000 * 10,
    int flags = 0x40,
  ]) async {
    final response = await rsApiCall(
      '/rsFiles/ExtraFileHash',
      params: {
        'localpath': localPath,
        'period': {'xstr64': period.toString()},
        'flags': flags,
      },
    );
    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<Map> extraFileStatus(String localPath) async {
    final response = await rsApiCall(
      '/rsFiles/ExtraFileStatus',
      params: {'localpath': localPath},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      print('Could not retrieve file status for $localPath');
    }
    return response['info'];
  }

  static Future<String> exportFileLink(
    String fileHash,
    int fileSize,
    String fileName,
  ) async {
    final params = {
      'fileHash': fileHash,
      'fileSize': fileSize,
      'fileName': fileName,
    };

    final response = await rsApiCall('/rsFiles/exportFileLink', params: params);
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception('Could not export file link. $params');
    }

    return response['link'];
  }

  static Future<Map> parseFilesLink(String link) async {
    final response =
        await rsApiCall('/rsFiles/parseFilesLink', params: {'link': link});
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception('Could not parse file link: $link');
    }

    return response['collection'];
  }

  static Future<void> requestFiles(Map collection) async {
    print('Requesting $collection');
    final response = await rsApiCall(
      '/rsFiles/requestFiles',
      params: {'collection': collection},
    );
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception('Files request failed.');
    }
  }

  static Future<void> setDownloadDirectory(String path) async {
    final response = await rsApiCall(
      '/rsFiles/setDownloadDirectory',
      params: {'path': path},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Error setting download directory. Response: $response');
    }
  }

  static Future<String> getDownloadDirectory() async {
    final response = await rsApiCall('/rsFiles/getDownloadDirectory');
    print('Download dir: $response');
    return response['retval'];
  }

  static Future<String> getPartialsDirectory() async {
    final response = await rsApiCall('/rsFiles/getPartialsDirectory');
    print('Partials dir: $response');
    return response['retval'];
  }

  static Future<List> getSharedDirectories() async {
    final response = await rsApiCall('/rsFiles/getSharedDirectories');
    print('Shared dirs: $response');
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Error getting shared directories. Response: $response');
    }
    return response['dirs'];
  }

  /// Remove directory from shared list
  ///
  /// [dir] Path of the directory to remove from shared list
  /// return false if something failed, true otherwhise
  static Future<bool> removeSharedDirectory(String dir) async {
    final response =
        await rsApiCall('/rsFiles/removeSharedDirectory', params: {'dir': dir});

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  static Future<void> setPartialsDirectory(String path) async {
    final response = await rsApiCall(
      '/rsFiles/setPartialsDirectory',
      params: {'path': path},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception(
        'Error setting partial download directory. Response: $response',
      );
    }
  }

  /// Add shared directory
  ///
  /// [filename] is the directory or file you want to share. [virtualname] is
  /// the name will be shown on RS, if null will be `basename(filename)`. For
  /// [shareflags] read RS documentation.
  ///
  /// return false if something failed or already shared, true if shared
  static Future<bool> addSharedDirectory(
    String filename, {
    required String virtualname,
    int shareflags = 0x80,
  }) async {
    print('Adding shared directory: ');

    final response = await rsApiCall(
      '/rsFiles/addSharedDirectory',
      params: {
        'dir': {
          'filename': filename,
          'virtualname': virtualname,
          'shareflags': shareflags,
        },
      },
    );

    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception(
        'Error adding shared directory $filename . $response\nMay be already added',
      );
    }
    return true;
  }

  static Future<List> fileDownloads() async {
    final response = await rsApiCall('/rsFiles/FileDownloads');
    return response['hashs'];
  }

  /// Request directory details, subsequent multiple call may be used to
  /// explore a whole directory tree.
  ///
  /// @param[out] details Storage for directory details
  /// @param[in] handle element handle 0 for root, pass the content of
  ///	DirDetails::child[x].ref after first call to explore deeper, be aware
  ///	that is not a real pointer but an index used internally by RetroShare.
  /// @param[in] NOTSUPORTED flags file search flags RS_FILE_HINTS_*
  /// @return false if error occurred, true otherwise
  static Future<DirDetails> requestDirDetails([int? handle]) async {
    Map response;
    if (handle == null) {
      response = await rsApiCall('/rsFiles/requestDirDetails');
    } else {
      response = await rsApiCall(
        '/rsFiles/requestDirDetails',
        params: {
          'handle': handle,
//        "handle": {"xstr64": handle}
        },
      );
    }
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Error requesting Dir Details.');
    }
    return DirDetails.fromJson(response['details']);
  }

  /// Get file details
  ///
  /// [hash] file identifier
  /// [hintflags] filtering hint ( RS_FILE_HINTS_UPLOAD|...| RS_FILE_HINTS_EXTRA
  /// |RS_FILE_HINTS_LOCAL )
  /// Out info storage for file information
  /// return true if file found, false otherwise
  ///
  /// WARNING: for a downloaded file could return false
  static Future<Map> fileDetails(String hash, [int hintflags = 0x10]) async {
    final response = await rsApiCall(
      '/rsFiles/FileDetails',
      params: {'hash': hash, 'hintflags': hintflags},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      print('File with hash $hash not found. Retval: ${response["retval"]}');
    }
    return response['info'];
  }

  /// Check if we already have a file
  ///
  /// [hash] SHA1 file identifier
  /// Return info storage for the possibly found file information
  static Future<Map?>? alreadyHaveFile(String hash) async {
    final response =
        await rsApiCall('/rsFiles/alreadyHaveFile', params: {'hash': hash});
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      return null;
    }
    return response['info'];
  }

  /// Remove file from extra fila shared list
  ///
  /// [hash] hash of the file to remove
  /// return false on error, true otherwise
  static Future<Map> extraFileRemove(String hash) async =>
      rsApiCall('/rsFiles/extraFileRemove', params: {'hash': hash});

  /// Search the whole reachable network for similar file
  ///
  /// An [RsPerceptualSearchResultEvent] is emitted when matching results
  ///	arrives from the network
  /// [localFilePath] path of local file to search for similarity on
  ///	the network
  /// [distance] maximum hamming distance tolerated to consider a file
  ///	similar
  /// Returns [searchId] storage for search id, useful to track search events
  ///	and retrieve search results
  static Future<int> perceptualSearchRequest(
    String localFilePath,
    int distance,
  ) async {
    print('Loading file for perceptual search: $localFilePath');
    final response = await rsApiCall(
      '/rsFiles/perceptualSearchRequest',
      params: {'localFilePath': localFilePath, 'distance': distance},
    );
    print('perceptualSearchRequest');
    print(response);
    final retval = response['retval'];
    if (retval is Map && retval['errorNumber'] != 0) {
      throw Exception(response['retval']);
    }
    return response['searchId'];
  }

  /// Force shared directories check
  /// [add_safe_delay] Schedule the check 20 seconds from now, to ensure to capture files written just now.
  static Future<void> forceDirectoryCheck({
    bool add_safe_delay = false,
  }) async =>
      rsApiCall(
        '/rsFiles/forceDirectoryCheck',
        params: {'add_safe_delay': add_safe_delay},
      );
}

class RsJsonApi {
  static Future<bool> isAuthTokenValid(AuthToken authToken) async {
    final reqUrl = getRetroshareServicePrefix();
    final response = await http.get(
      Uri.parse('$reqUrl/RsJsonApi/getAuthorizedTokens'),
      headers: {
        HttpHeaders.authorizationHeader:
            'Basic ${base64.encode(utf8.encode(authToken.toString()))}',
      },
    );

    if (response.statusCode == 200) {
      return true;
    }
    return false;
  }

  static Future<bool> checkExistingAuthTokens(
    String locationId,
    String password,
    AuthToken authToken,
  ) async {
    final reqUrl = getRetroshareServicePrefix();
    final response = await http.get(
      Uri.parse('$reqUrl/RsJsonApi/getAuthorizedTokens'),
      headers: {
        HttpHeaders.authorizationHeader:
            'Basic ${base64.encode(utf8.encode('$locationId:$password'))}',
      },
    );

    if (response.statusCode == 200) {
      for (final LinkedHashMap<String, dynamic> token
          in json.decode(response.body)['retval']) {
        if (token['key'] + ':' + token['value'] == authToken.toString()) {
          return true;
        }
      }
      await authorizeNewToken(locationId, password, authToken);
      return true;
    } else if (response.statusCode == 401) {
      return false;
    }

    throw Exception('Failed to load response');
  }

  static Future<void> authorizeNewToken(
    String locationId,
    String password,
    AuthToken authToken,
  ) async {
    final reqUrl = getRetroshareServicePrefix();
    final response = await http.post(
      Uri.parse('$reqUrl/RsJsonApi/authorizeUser'),
      body: json.encode({'token': authToken.toString()}),
      headers: {
        HttpHeaders.authorizationHeader:
            'Basic ${base64.encode(utf8.encode('$locationId:$password'))}',
        HttpHeaders.contentTypeHeader: 'application/json',
      },
    );

    if (response.statusCode == 200) {
      return;
    }

    throw Exception('Failed to load response');
  }

  static Future<Map> version() async {
    final methods = await rsApiCall('/rsJsonApi/getMethods');
    print('DEBUG: Available API methods: $methods');
    return rsApiCall('/rsJsonApi/version');
  }
}

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

class RsConfig {
  static Future<Map> getMaxDataRates() async {
    final response = await rsApiCall('/rsConfig/getMaxDataRates');
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) print('Could not get data rates');
    final rates = {'inKb': response['inKb'], 'outKb': response['outKb']};
    return rates;
  }

  static Future<Map> setMaxDataRates(int downKb, int upKb) async {
    final response = await rsApiCall(
      '/rsConfig/setMaxDataRates',
      params: {
        'inKb': downKb,
        'outKb': upKb,
        'inKbWhenIdle': 0,
        'outKbWhenIdle': 0,
      },
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) print('Could not set data rates $response');
    return response;
  }
}

// ----------------------------------------------------------------------------
// Circles
// ----------------------------------------------------------------------------

class RsGxsCircles {
  /// Create new circle
  ///
  /// [circleName] String containing cirlce name
  /// [circleType] Circle type
  /// [restrictedId] Optional id of a pre-existent circle that see the
  ///	created circle. Meaningful only if circleType == EXTERNAL, must be null
  ///	in all other cases.
  /// [authorId] Optional author of the circle.
  /// [gxsIdMembers] GXS ids of the members of the circle.
  /// [localMembers] PGP ids of the members if the circle.
  /// Returns circleId Optional storage to output created circle id
  static Future<String> createCircle(
    String circleName,
    RsGxsCircleType circleType, {
    String? restrictedId,
    List<String>? gxsIdMembers,
    List<String>? localMembers,
  }) async {
    final response = await rsApiCall(
      '/rsGxsCircles/createCircle',
      params: {
        'circleName': circleName,
        'circleType': circleType.index,
        'restrictedId': restrictedId,
        'gxsIdMembers': gxsIdMembers,
        'localMembers': localMembers,
      },
    );

    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Error creating circle $circleName . $response');
    }

    return response['circleId'];
  }

  /// Request circle membership, or accept circle invitation
  ///
  /// [ownGxsId] Id of own identity to introduce to the circle.
  /// Default value actual [authIdentityId]
  /// [circleId] Id of the circle to which ask for inclusion
  /// return false if something failed, true otherwhise
  static Future<bool> requestCircleMembership(
    String id,
    String circleId, [
    String? ownGxsId,
  ]) async {
    if (circleId.isEmpty) {
      throw Exception('Invalid circle ID');
    }

    final response = await rsApiCall(
      '/rsGxsCircles/requestCircleMembership',
      params: {'ownGxsId': ownGxsId ?? id, 'circleId': circleId},
    );

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Invite identities to circle (admin key is required)
  ///
  /// [identities] ids of the identities to invite
  /// [circleId] Id of the circle you own and want to invite ids in
  /// return false if something failed, true otherwhise
  static Future<bool> inviteIdsToCircle(
    List<String> identities,
    String circleId,
  ) async {
    print('Inviting ids $identities to circleId');
    final response = await rsApiCall(
      '/rsGxsCircles/inviteIdsToCircle',
      params: {'identities': identities, 'circleId': circleId},
    );

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Get circle details. Memory cached
  ///
  /// [id] Id of the circle
  /// return details Storage for the circle details
  static Future<Map<String, dynamic>> getCircleDetails(String id) async {
    if (id.isEmpty) throw Exception('Invalid circle ID');

    final response =
        await rsApiCall('/rsGxsCircles/getCircleDetails', params: {'id': id});

    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception("Can't retrieve details for $id");
    }
    return response['details'];
  }

  /// @brief Get circles summaries list.
  ///
  /// return circles list where to store the circles summaries
  static Future<List<dynamic>> getCirclesSummaries() async {
    print('Starting getCirclesSummaries');
    final response = await rsApiCall('/rsGxsCircles/getCirclesSummaries');
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Could not retrieve circle summaries');
    }
    return response['circles'];
  }

  /// Get circles information
  ///
  /// [circlesIds] ids of the circles of which to get the informations
  /// return [circlesInfo] storage for the circles informations
  static Future<List<dynamic>> getCirclesInfo(List<String> circlesIds) async {
    final response = await rsApiCall(
      '/rsGxsCircles/getCirclesInfo',
      params: {'circlesIds': circlesIds},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception("Can't getCirclesInfo for $circlesIds");
    }
    return response['circlesInfo'];
  }

  /// Remove identities from circle (admin key is required)
  ///
  /// [identities] ids of the identities to remove from the invite list
  /// [circleId] Id of the circle you own and want to revoke identities
  /// return false if something failed, true otherwhise
  static Future<bool> revokeIdsFromCircle(
    List<String> identities,
    String circleId,
  ) async {
    final response = await rsApiCall(
      '/rsGxsCircles/revokeIdsFromCircle',
      params: {'identities': identities, 'circleId': circleId},
    );

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Leave given circle
  ///
  /// [ownGxsId] Own id to remove from the circle. Default value actual [authIdentityId]
  /// [circleId] Id of the circle to leave
  /// return false if something failed, true otherwhise
  static Future<bool> cancelCircleMembership(
    String id,
    String circleId, [
    String? ownGxsId,
  ]) async {
    final response = await rsApiCall(
      '/rsGxsCircles/cancelCircleMembership',
      params: {'ownGxsId': ownGxsId ?? id, 'circleId': circleId},
    );

    final retval = response['retval'];
    return (retval is bool && retval) || (retval is int && retval == 1);
  }

  /// Edit own existing circle
  ///
  /// Parameter inout [circleInfo] is the same object that the recieved with
  /// getCirclesInfo. Circle data with modifications, storage for data
  ///	updatedad during the operation.
  /// Return a circle info.
  static Future<Map<String, dynamic>> editCircle(
    Map<String, dynamic> circleInfo,
  ) async {
    final response = await rsApiCall(
      '/rsGxsCircles/editCircle',
      params: {'cData': circleInfo},
    );
    final retval = response['retval'];
    if (!((retval is bool && retval) || (retval is int && retval == 1))) {
      throw Exception('Could not edit editCircle $circleInfo');
    }
    return response['cData'];
  }
}

// ----------------------------------------------------------------------------
// UTILITIES
// ----------------------------------------------------------------------------

/// Register event specifically for chat messages
/// This function add code to deserialization of
/// the message, automatizing the process.
Future<Future<StreamSubscription<String>?>> eventsRegisterChatMessage({
  Function? listenCb,
  Function? onError,
  AuthToken? authToken,
}) async {
  return RsEvents.registerEventsHandler(
    RsEventType.CHAT_MESSAGE,
    // Update the callback signature: remove the Event type hint
    // The callback now receives the streamSubscription and the decoded JSON event data
    (streamSubscription, eventData) {
      // Deserialize the message directly from the eventData
      ChatMessage? chatMessage;
      if (eventData != null && eventData['mChatMessage'] != null) {
        // Check if eventData is not null and contains mChatMessage
        chatMessage = ChatMessage.fromJson(eventData['mChatMessage']);
      }
      // Pass the original JSON and the deserialized ChatMessage to the listen callback
      if (listenCb != null) listenCb(eventData, chatMessage);
    },
    authToken,
    onError: onError,
  );
}

//IdentityUtils

Future<List<Identity>> getOwnIdentities(AuthToken authToken) async {
  final ownIdsList = <Identity>[];

  /// fetch the signed Identity
  final List<dynamic> respSigned =
      await RsIdentity.getOwnSignedIdentity(authToken);

  respSigned.toSet().forEach((id) {
    if (id != null && isNullCheck(id)) {
      ownIdsList.add(Identity(mId: id, signed: true, isContact: false));
    }
  });

  /// fetch the Unsigned Identity
  final List<dynamic> respPseudonymous =
      await RsIdentity.getOwnPseudonimousIds(authToken);

  respPseudonymous.toSet().forEach((id) {
    if (id != null && isNullCheck(id)) {
      ownIdsList.add(Identity(mId: id, signed: false, isContact: false));
    }
  });

  for (var x = 0; x < ownIdsList.length; x++) {
    final resp = await getIdDetails(ownIdsList[x].mId, authToken);
    if (resp.item1) ownIdsList[x] = resp.item2;
  }

  return ownIdsList;
}

Future<({bool item1, Identity item2})> getIdDetails(
  String id,
  AuthToken authToken,
) async {
  final response = await RsIdentity.getIdDetails(id, authToken);

  final retval = response['retval'];
  if ((retval is bool && retval) || (retval is int && retval == 1)) {
    final identity = Identity(
      mId: id,
      name: response['details']['mNickname'],
      avatar: response['details']['mAvatar']['mData']['base64'] != null &&
              response['details']['mAvatar']['mData']['base64']
                  .toString()
                  .isNotEmpty
          ? response['details']['mAvatar']['mData']['base64'].toString()
          : null,
      signed: response['details']['mPgpId'] != '0000000000000000',
      pgpId: response['details']['mPgpId'],
      isContact: false,
        status: (response['details']['mPresence'] ?? response['details']['presence'] ?? response['details']['mPresenceData']) != null
            ? (response['details']['mPresence'] ?? response['details']['presence'] ?? response['details']['mPresenceData'])['mStatus'] ?? 
              (response['details']['mPresence'] ?? response['details']['presence'] ?? response['details']['mPresenceData'])['status'] ?? 
              (response['details']['mPresence'] ?? response['details']['presence'] ?? response['details']['mPresenceData'])['mPresenceStatus'] ?? 0
            : 0,
    );

    return (item1: true, item2: identity);
  }
  return (
    item1: false,
    item2: const Identity(mId: '', signed: false, isContact: false)
  );
}

// Identities that are not contacts do not have loaded avatars
Future<(List<Identity>, List<Identity>, List<Identity>)> getAllIdentities(
  AuthToken authToken,
) async {
  final response = await RsIdentity.getIdentitiesSummaries(authToken);
  final ids = <String>{};
  for (final id in response) {
    if (id['mGroupId'] != null) {
      ids.add(id['mGroupId']);
    }
  }
  final response2 = await RsIdentity.getIdentitiesInfo(ids.toList(), authToken);

  final notContactIds = <Identity>[];
  final contactIds = <Identity>[];
  final signedContactIds = <Identity>[];
  final ownIds = <Identity>[];
  final idsInfo = response2;
  for (var i = 0; i < idsInfo.length; i++) {
    if (idsInfo[i]['mIsAContact'] == true &&
        idsInfo[i]['mMeta']['mSubscribeFlags'] != 7) {
      // get the  contact IDs and Unknown Ids
      // Use Dart record type (Identity, bool) instead of Tuple2
      final knownIden = await getKnownIdentity(idsInfo[i], authToken);
      // Access elements using $1, $2 etc. for unnamed records
      if (knownIden.$2) {
        signedContactIds.add(knownIden.$1);
      }
      contactIds.add(knownIden.$1);
    } else if (idsInfo[i]['mMeta']['mSubscribeFlags'] == 7) {
      // ownIdentity
      ownIds.add(
        Identity(
          mId: idsInfo[i]['mMeta']['mGroupId'],
          signed: idsInfo[i]['mPgpId'] != '0000000000000000',
          pgpId: idsInfo[i]['mPgpId'],
          name: idsInfo[i]['mMeta']['mGroupName'],
          isContact: false,
          originator: idsInfo[i]['mMeta'] != null ? idsInfo[i]['mMeta']['mOriginator'] : null,
          status: (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData']) != null
              ? (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['mStatus'] ?? 
                (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['status'] ?? 
                (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['mPresenceStatus'] ?? 0
              : 0,
        ),
      );
    } else {
      // unknown Identity
      notContactIds.add(
        Identity(
          mId: idsInfo[i]['mMeta']['mGroupId'],
          signed: idsInfo[i]['mPgpId'] != '0000000000000000',
          pgpId: idsInfo[i]['mPgpId'],
          name: idsInfo[i]['mMeta']['mGroupName'],
          isContact: false,
          originator: idsInfo[i]['mMeta'] != null ? idsInfo[i]['mMeta']['mOriginator'] : null,
          status: (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData']) != null
              ? (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['mStatus'] ?? 
                (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['status'] ?? 
                (idsInfo[i]['mPresence'] ?? idsInfo[i]['presence'] ?? idsInfo[i]['mPresenceData'])['mPresenceStatus'] ?? 0
              : 0,
        ),
      );
    }
  }
  // sort the unknown Identity by name
  notContactIds.sort((id1, id2) {
    final name1 = id1.name;
    final name2 = id2.name;

    if (name1 != null && name2 != null) {
      return name1.compareTo(name2);
    } else {
      return name1 == null
          ? -1
          : (name2 == null ? 1 : 0); // Handle nulls explicitly
    }
  });

  // Return a Dart record instead of Tuple3
  return (
    signedContactIds,
    contactIds,
    notContactIds,
  );
}

// Change return type to Dart record (Identity, bool)
Future<(Identity, bool)> getKnownIdentity(
  dynamic idsInfo,
  AuthToken authToken,
) async {
  Identity identity;
  var success = true;
  do {
    // Use the record type returned by getIdDetails
    final tuple = await getIdDetails(idsInfo['mMeta']['mGroupId'], authToken);
    success = tuple.item1; // Access named field .item1
    identity = tuple.item2; // Access named field .item2
  } while (!success);

  // This is because sometimes,
  // the returning Id of [getIdDetails], that is a
  // result of call 'torsIdentity/getIdDetails', return identity details, from the cache
  // So sometimes the avatar are not updated, instead of in rsIdentity/getIdentitiesInfo, where they are
  if (identity.avatar == '' && idsInfo['mImage']['mData']['base64'] != '') {
    identity = identity.copyWith(avatar: idsInfo['mImage']['mData']['base64']);
  }

  if (idsInfo['mPresence'] != null || idsInfo['presence'] != null || idsInfo['mPresenceData'] != null) {
    final p = idsInfo['mPresence'] ?? idsInfo['presence'] ?? idsInfo['mPresenceData'];
    identity = identity.copyWith(status: p['mStatus'] ?? p['status'] ?? p['mPresenceStatus'] ?? 0);
  }

  identity = identity.copyWith(
    isContact: true,
    originator: idsInfo['mMeta'] != null ? idsInfo['mMeta']['mOriginator'] : null,
  );

  // Return a Dart record instead of Tuple2
  return (identity, identity.signed);
}

bool isNullCheck(String s) {
  return s != '00000000000000000000000000000000';
}

/// Used to know if a directory is in the list of getSharedDirectories
Future<bool> isDirectoryAlreadyShared(String filePath) async {
  for (final share in await RsFiles.getSharedDirectories()) {
    if (share['filename'] == filePath) {
      print('$filePath already shared');
      return true;
    }
  }
  return false;
}

/// Search recursivelly on the RS directory tree until find a specified path or
/// file returning it information
Future<DirDetails?>? findAFileOnDiretoryTree(
  String filePath, [
  int handle = 0,
]) async {
  final directory = await RsFiles.requestDirDetails(handle);
  if (directory.children.isNotEmpty) {
    // First check to see if requested filePath is in this directory
    for (final child in directory.children) {
      if (child['name'] == filePath) {
        return RsFiles.requestDirDetails(child['handle']);
      }
    }
    // If isn't start the recursive search
    for (final child in directory.children) {
      final childDetails =
          await findAFileOnDiretoryTree(filePath, child['handle']);
      if (childDetails != null) return childDetails;
    }
  }
  return null;
}

Future<bool> waitUntilOnline(
  String sslId,
  AuthToken authToken, [
  int attempts = 10,
]) async {
  // wait a bit until the peer accepts us
  for (; attempts >= 0; attempts--) {
    try {
      final ids = await RsPeers.getOnlineList(authToken);
      if (ids.contains(sslId)) return true; // done

      // else => no news yet => retry
      await RsPeers.connectAttempt(sslId, authToken).catchError((_) {});

      if (attempts == 0) {
        // do not wait on the last iteration
        return false;
      }

      // else => retry
      await Future.delayed(const Duration(seconds: 5));
    } catch (err) {
      await Future.delayed(const Duration(seconds: 5));
    }
  }
  return false;
}

Future<bool> waitUntilSent(String msgId, [int attempts = 5]) async {
  try {
    for (; attempts >= 0; attempts--) {
      final response = await RsMsgs.getMessage(msgId);
      if (response == null) {
        return false;
      } else if ((response['msgflags'] & RS_MSG_PENDING) != 0) {
        // still pending
        await Future.delayed(const Duration(seconds: 5));
        continue;
      }
      return true;
    }
    return false;
  } catch (err) {
    return false;
  }
}

/// Execute befriending with a Tier1
///
/// It try to befriend with a [hostname] on the [baseUrl]
/// `http://$hostname.$baseUrl/GetRetroshareInvite` and
/// `http://$hostname.$baseUrl/acceptInvite`
///
/// Return true if success false otherwhise printing an error.
Future<bool> befriendTier1(
  AuthToken authToken, {
  String hostname = 'michelangiolillo',
  String baseUrl = 'elrepo.io/rsPeers',
}) async {
  print('Befriending $hostname');

  // Get RS invite process
  var reqUrl = 'http://$hostname.$baseUrl/GetRetroshareInvite';
  try {
    final response = await http.get(Uri.parse(reqUrl));
    Map decoded;
    if (response.statusCode == 200) {
      decoded = jsonDecode(response.body);
    } else {
      print(
        'Error receiving Tier1 invite. Status code: ${response.statusCode}',
      );
      return false;
    }
    final String tier1Cert = decoded['retval'];
    await RsPeers.acceptInvite(authToken, tier1Cert);
  } catch (error) {
    print('Cannot reach $hostname. Error: $error');
    return false;
  }

  // Send RS invite process
  final myInvite = await RsPeers.getOwnCert(authToken);
  reqUrl = 'http://$hostname.$baseUrl/acceptInvite';
  final jsonCert = {'invite': myInvite};
  try {
    final response =
        await http.post(Uri.parse(reqUrl), body: jsonEncode(jsonCert));
    if (response.statusCode != 200) {
      print('Error sending cert to $hostname. Code ${response.statusCode}');
    }
    return true;
  } catch (error) {
    print('Connection to $hostname failed. Error: $error ');
  }
  return false;
}

Future<void> addFriends(AuthToken authToken, List peers) async {
  Map peer;
  for (peer in peers) {
    print('Adding Friend: $peer');
    final sslId = peer['mSslId'];
    final alreadyFriend = await RsPeers.isFriend(sslId, authToken);
    if (!alreadyFriend) {
      final pgpFingerprint = peer['mPgpFingerprint'];
      final pgpId = pgpFingerprint.substring(pgpFingerprint.length - 16);
      final uri = peer['mLocator']['urlString'];
      final parsedUri = Uri.parse(uri);
      final details = {
        'localAddr': parsedUri.host,
        'localPort': parsedUri.port,
      };
      await RsPeers.addSslOnlyFriend(authToken, sslId, pgpId, details);
    }
  }
}

/// Function that get peers on local network and add its as friends
Future<void> broadcastPromiscuity(AuthToken authToken) async {
  print('Runing Broadcarst Promisquity');
  final peers = await RsBroadcastDiscovery.getDiscoveredPeers();
  if (peers.isNotEmpty) {
    print('Peers found: $peers');
    await addFriends(authToken, peers);
  }
}
