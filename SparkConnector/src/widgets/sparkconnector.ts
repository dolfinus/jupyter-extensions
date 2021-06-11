import { ReactWidget } from '@jupyterlab/apputils';
import { JSONObject } from '@lumino/coreutils';
import {
  ConnectionStatus,
  IComm,
  IKernelConnection,
} from '@jupyterlab/services/lib/kernel/kernel';
import { INotebookTracker, NotebookPanel } from '@jupyterlab/notebook';
import { KernelMessage } from '@jupyterlab/services';

import { NotAttachedState } from './notattached';
import { LoadingState } from './loading';
import { ConfigurationState } from './configuring';
import { ConnectingState } from './connecting';
import { ConnectedState } from './connected';
import { ConnectFailedState } from './connectfailed';

export interface IState {
  render(): JSX.Element;
  name(): string;
}

export interface SparkOpt {
  name: string;
  value: string;
}

export interface SparkconnectMetadata {
  bundled_options: Array<string>;
  list_of_options: Array<SparkOpt>;
}

class UndefinedStates {
  loading: LoadingState<IState>;
  notattached: LoadingState<IState>;

  constructor() {
    this.notattached = new NotAttachedState();
    this.loading = new LoadingState();
  }
}

class NotebookStates {
  configuring: ConfigurationState<IState>;
  connecting: ConnectingState<IState>;
  connected: ConnectedState<IState>;
  connectfailed: ConnectFailedState<IState>;

  constructor() {
    this.configuring = new ConfigurationState();
    this.connecting = new ConnectingState();
    this.connected = new ConnectedState();
    this.connectfailed = new ConnectFailedState();
  }
}

interface NotebookComm {
  notebook: NotebookPanel;
  states: NotebookStates;
  comm: IComm;
}

class StateHandler {
  private states: Map<string, NotebookComm>;
  private notebooks: INotebookTracker;

  constructor(notebooks: INotebookTracker) {
    this.states = new Map<string, NotebookComm>();
    this.notebooks = notebooks;
  }

  /**
   * we can only communicate with 1 notebook at a time
   * @param notebookId
   */
  canOpen(notebookId: string): boolean {
    return notebookId == this.notebooks.currentWidget.id;
  }

  open(notebookId: string): NotebookComm {
    const notebookComm: NotebookComm = this.states.get(notebookId);
    if (notebookComm.comm.isDisposed) {
      console.log('conn disposed ' + notebookId);
      return notebookComm;
    }
    /* TODO this is invalid for our current backend
     notebookComm.comm.send({
              type: 'action',
              action: 'sparkconn-action-open',
            }).done.then(() => {
       console.log('conn restored ' + notebookId);
     });
     */
  }

  create(notebook: NotebookPanel): NotebookComm {
    let kernel = notebook.sessionContext.session.kernel;

    let comm = kernel.createComm('SparkConnector');
    comm
      .open({
        type: 'action',
        action: 'sparkconn-action-open',
      })
      .done.then(() => {
        this.states.set(notebook.id, notebookComm);
        console.log('conn opened ' + notebook.title.label);
      });
    comm.onClose = () => {
      this.states.delete(notebook.id);
      console.log('conn closed ' + notebook.title.label);
    };

    let states = new NotebookStates();

    let notebookComm: NotebookComm = {
      notebook: notebook,
      states: states,
      comm: comm,
    };

    return notebookComm;
  }

  has(notebookId: string): boolean {
    return this.states.has(notebookId);
  }

  close(notebookId: string): void {
    this.states.delete(notebookId);
    console.log('conn closed ' + notebookId);
  }

  clear(): void {
    this.states.clear();
    console.log('conns cleared');
  }
}

/**
 * A class that exposes the git plugin Widget.
 */
export class SparkConnector extends ReactWidget {
  private statehandler: StateHandler;
  private notebooks: INotebookTracker;
  private currentState: IState;

  /**
   * Construct a console panel.
   */
  constructor(notebooks: INotebookTracker) {
    super();
    this.addClass('jp-SparkConnector');

    this.notebooks = notebooks;
    this.statehandler = new StateHandler(notebooks);

    this.initStateHandling();
  }

  updateCurrent(state: IState) {
    this.currentState = state;
    this.update();
  }

  initStateHandling(): void {
    let undefinedStates = new UndefinedStates();
    this.currentState = undefinedStates.notattached;
    this.notebooks.currentChanged.connect(
      (sender: any, nbPanel: NotebookPanel) => {
        if (!nbPanel) {
          // if not NotebookPanel has been opened
          this.updateCurrent(undefinedStates.notattached);
          return;
        }

        this.updateCurrent(undefinedStates.loading);

        nbPanel.sessionContext.ready.then(() => {
          const title = nbPanel.title.label;
          const kernel = nbPanel.sessionContext.session.kernel;
          if (this.statehandler.has(nbPanel.id)) {
            // if we already have a comm, connectionStatusChanged wont be triggered,
            // manually send sparkconn-action-open
            this.statehandler.open(nbPanel.id);
          } else if (kernel.connectionStatus == 'connected') {
            // if we do not have comm and kernel connected, connectionStatusChanged wont be triggered
            // and e need to reconnect
            console.log('notebook reconnecting ' + title);
            kernel.reconnect().then();
          }

          kernel.connectionStatusChanged.connect(
            (conn: IKernelConnection, status: ConnectionStatus) => {
              if (status == 'connected') {
                console.log(
                  'SparkConnector: Notebook Kernel Connected ',
                  title
                );
                const notebookComm = this.statehandler.create(nbPanel);
                notebookComm.comm.onMsg = (msg: KernelMessage.ICommMsgMsg) => {
                  if (
                    this.notebooks.currentWidget == null ||
                    this.notebooks.currentWidget.id != nbPanel.id
                  ) {
                    return;
                  }
                  this.onCommMessage(
                    msg,
                    notebookComm.notebook,
                    notebookComm,
                    kernel
                  );
                };
              } else if (status == 'connecting') {
                this.updateCurrent(undefinedStates.loading);
                this.statehandler.close(nbPanel.id);
              } else {
                this.updateCurrent(undefinedStates.notattached);
              }
            }
          );
        });
      }
    );
  }

  /*
    Handle messages from the Kernel extension.
  */
  onCommMessage(
    message: KernelMessage.ICommMsgMsg,
    notebookPanel: any,
    notebookComm: NotebookComm,
    kernel: IKernelConnection
  ) {
    console.debug(
      `SparkConnector: Comm Message ${message.content.data.msgtype} for notebook ${notebookPanel}:\n`,
      message.content.data
    );
    const data: any = message.content.data;
    const title = notebookPanel.title.label; // TODO Remove this
    let undefinedStates = new UndefinedStates();
    switch (data.msgtype) {
      case 'sparkconn-action-open':
        const page = message.content.data.page;
        if (page == 'sparkconn-config') {
          const currentConfig =
            this.getCurrentConfigFromNotebook(notebookPanel);
          notebookComm.states.configuring.init(
            title,
            data.maxmemory as string,
            data.sparkversion as string,
            data.cluster as string,
            currentConfig,
            data.availableoptions as JSONObject,
            data.availablebundles as JSONObject
          );
          // Connect button clicked
          notebookComm.states.configuring.onConnect.connect(
            (
              configuring: ConfigurationState<IState>,
              connectMessage: JSONObject
            ) => {
              
              notebookComm.states.connecting.init(
                title,
                data.sparkversion as string,
                data.cluster as string
              );
              this.updateCurrent(notebookComm.states.connecting);
              this.setCurrentConfigToNotebook(
                notebookPanel,
                connectMessage['metadata'] as any
              );
              notebookComm.comm.send({
                type: 'action',
                action: 'sparkconn-action-connect',
                'action-data': { options: connectMessage['options'] },
              });
            }
          );
          this.updateCurrent(notebookComm.states.configuring);
        } else if (page == 'sparkconn-auth') {
          // TODO switch to authentication page
        } else if (page == 'sparkconn-connected') {
          // The kernel sends this page when a comm is opened, but the
          // user is already connected. It subsequently also sends a msgtype: sparkconn-connected,
          // so we don't do anything here
        }
        break;

      case 'sparkconn-connected':
        notebookComm.states.connected.init(
          title,
          data.config.sparkhistoryserver as string
        );
        //when the sparkconnector goes into connected state,
        //kernel restarts are the equivalent of a reload of the entire connection
        kernel.statusChanged.connect((_, status) => {
          // TODO What is this:
          if (status == 'restarting') {
            kernel.reconnect().then(() => {
              this.statehandler = new StateHandler(this.notebooks);
              this.initStateHandling();
            });
          }
        });
        notebookComm.states.connected.onReconfigure.connect(() => {
          // Connect button clicked
          this.updateCurrent(undefinedStates.loading);
          notebookComm.comm.send({
            type: 'action',
            action: 'sparkconn-action-disconnect',
          });
          notebookComm.notebook.sessionContext.session.kernel.restart();
        });
        this.updateCurrent(notebookComm.states.connected);
        break;

      case 'sparkconn-config':
        // Sent by kernel on successful authentication
        // TODO Switch to Config Page
        break;

      case 'sparkconn-connect-error':
        notebookComm.states.connectfailed.init(title, data.error as string);
        notebookComm.states.connectfailed.onReconfigure.connect(() => {
          this.updateCurrent(undefinedStates.loading);
          notebookComm.comm.send({
            action: 'sparkconn-action-disconnect',
          });
          // Restart the kernel, because SparkContexts are cached,
          // we need to restart to do a clean retry again
          notebookComm.notebook.sessionContext.session.kernel.restart();
        });
        this.updateCurrent(notebookComm.states.connectfailed);
        break;

      case 'sparkconn-action-follow-log':
        if (this.currentState.name() == 'connected') {
          notebookComm.states.connected.log(data.msg as string);
        } else if (this.currentState.name() == 'connecting') {
          notebookComm.states.connecting.log(data.msg as string);
        }
        break;

      default:
        console.error(
          'SparkConnector: Received an unknown msgtype from kernel:',
          message
        );
        break;
    }
  }

  getCurrentConfigFromNotebook(notebookPanel: NotebookPanel) {
    let currentConfig;
    if (notebookPanel.model.metadata.has('sparkconnect')) {
      currentConfig = notebookPanel.model.metadata.get(
        'sparkconnect'
      ) as unknown as SparkconnectMetadata;
    } else {
      currentConfig = {
        bundled_options: [],
        list_of_options: [],
      } as SparkconnectMetadata;
    }
    return currentConfig;
  }

  setCurrentConfigToNotebook(
    notebookPanel: NotebookPanel,
    config: SparkconnectMetadata
  ) {
    notebookPanel.model.metadata.set('sparkconnect', config as any);
  }

  render(): JSX.Element {
    return this.currentState.render();
  }
}
