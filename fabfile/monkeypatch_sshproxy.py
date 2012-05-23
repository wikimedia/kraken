import sys
import socket

import getpass
import paramiko as ssh
from paramiko.resource import ResourceManager

from fabric import network
from fabric import state as s

def connect_forward(gw, host, port, user):
    """
    Create a different connect that works with a gateway. We really need to
    create the socket and destroy it when the connection fails and then retry
    the connect.
    """
    client = ForwardSSHClient()
    while True:
        # Load known host keys (e.g. ~/.ssh/known_hosts) unless user says not to.
        if not s.env.disable_known_hosts:
            client.load_system_host_keys()
        # Unless user specified not to, accept/add new, unknown host keys
        if not s.env.reject_unknown_hosts:
            client.set_missing_host_key_policy(ssh.AutoAddPolicy())
        
        sock = gw.get_transport().open_channel('direct-tcpip', (host, int(port)), ('', 0))
        try:
            client.connect(host, sock, int(port), user, s.env.password,
                           key_filename=s.env.key_filename, timeout=10)
            client._sock_ = sock
            return client
        except (
            ssh.AuthenticationException,
            ssh.PasswordRequiredException,
            ssh.SSHException
        ), e:
            if e.__class__ is ssh.SSHException and password:
                network.abort(str(e))
            
            s.env.password = network.prompt_for_password(s.env.password)
            sock.close()
        
        except (EOFError, TypeError):
            # Print a newline (in case user was sitting at prompt)
            print('')
            sys.exit(0)
        # Handle timeouts
        except socket.timeout:
            network.abort('Timed out trying to connect to %s' % host)
        # Handle DNS error / name lookup failure
        except socket.gaierror:
            network.abort('Name lookup failed for %s' % host)
        # Handle generic network-related errors
        # NOTE: In 2.6, socket.error subclasses IOError
        except socket.error, e:
            network.abort('Low level socket error connecting to host %s: %s' % (
                host, e[1])
            )

class ForwardSSHClient(ssh.SSHClient):
    """
    Override the default ssh.SSHClient to make it accept a socket as an extra argument,
    instead of creating one of its own.
    """
    def connect(self, hostname, sock, port=22, username=None, password=None, pkey=None,
                key_filename=None, timeout=None, allow_agent=True, look_for_keys=True):
        t = self._transport = ssh.Transport(sock)
        
        if self._log_channel is not None:
            t.set_log_channel(self._log_channel)
        
        t.start_client()
        ResourceManager.register(self, t)
        
        server_key = t.get_remote_server_key()
        keytype = server_key.get_name()
        
        our_server_key = self._system_host_keys.get(hostname, {}).get(keytype, None)
        if our_server_key is None:
            our_server_key = self._host_keys.get(hostname, {}).get(keytype, None)
        if our_server_key is None:
            # will raise exception if the key is rejected; let that fall out
            self._policy.missing_host_key(self, hostname, server_key)
            # if the callback returns, assume the key is ok
            our_server_key = server_key
        
        if server_key != our_server_key:
            raise ssh.BadHostKeyException(hostname, server_key, our_server_key)
        
        if username is None:
            username = getpass.getuser()
        
        if key_filename is None:
            key_filenames = []
        elif isinstance(key_filename, (str, unicode)):
            key_filenames = [ key_filename ]
        else:
            key_filenames = key_filename
        self._auth(username, password, pkey, key_filenames, allow_agent, look_for_keys)

class GatewayConnectionCache(network.HostConnectionCache):
    _gw = None
    def __getitem__(self, key):
        gw = s.env.get('gateway')
        if gw is None:
            return super(GatewayConnectionCache, self).__getitem__(key)
        
        gw_user, gw_host, gw_port = network.normalize(gw)
        if self._gw is None:
            # Normalize given key (i.e. obtain username and port, if not given)
            self._gw = network.connect(gw_user, gw_host, gw_port)
        
        # Normalize given key (i.e. obtain username and port, if not given)
        user, host, port = network.normalize(key)
        # Recombine for use as a key.
        real_key = network.join_host_strings(user, host, port)
        
        # If not found, create new connection and store it
        if real_key not in self:
            self[real_key] = connect_forward(self._gw, host, port, user)
        
        # Return the value either way
        return dict.__getitem__(self, real_key)

_c = s.connections = GatewayConnectionCache()
from fabric import operations
operations.connections = _c
