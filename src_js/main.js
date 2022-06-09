import r from '@hat-open/renderer';
import * as u from '@hat-open/util';
import * as juggler from '@hat-open/juggler';


import '../src_scss/main.scss';


const defaultState = {
    remote: null
};


let app = null;


function main() {
    const root = document.body.appendChild(document.createElement('div'));
    r.init(root, defaultState, vt);
    app = new juggler.Application(null, 'remote');
}


function setRank(cid, rank) {
    app.send({type: 'set_rank', payload: {
        cid: cid,
        rank: rank
    }});
}


function vt() {
    if (!r.get('remote'))
        return  ['div.monitor'];
    return ['div.monitor',
        localComponentsVt(),
        globalComponentsVt()
    ];
}


function localComponentsVt() {
    const components = r.get('remote', 'local_components');
    return ['div',
        ['h1', 'Local components'],
        ['table',
            ['thead',
                ['tr',
                    ['th.col-id', 'CID'],
                    ['th.col-name', 'Name'],
                    ['th.col-group', 'Group'],
                    ['th.col-data', 'Data'],
                    ['th.col-rank', 'Rank']
                ]
            ],
            ['tbody', components.map(({cid, name, group, data, rank}) =>
                ['tr',
                    ['td.col-id', String(cid)],
                    ['td.col-name', name || ''],
                    ['td.col-group', group || ''],
                    ['td.col-data', data || ''],
                    ['td.col-rank',
                        ['div',
                            ['button', {
                                on: {
                                    click: _ => setRank(cid, rank - 1)
                                }},
                                ['span.fa.fa-chevron-left']
                            ],
                            ['div', String(rank)],
                            ['button', {
                                on: {
                                    click: _ => setRank(cid, rank + 1)
                                }},
                                ['span.fa.fa-chevron-right']
                            ]
                        ]
                    ]
                ]
            )]
        ]
    ];
}


function globalComponentsVt() {
    const components = r.get('remote', 'global_components');
    return ['div',
        ['h1', 'Global components'],
        ['table',
            ['thead',
                ['tr',
                    ['th'],
                    ['th'],
                    ['th'],
                    ['th'],
                    ['th'],
                    ['th'],
                    ['th', { props: { colspan: '2' } }, 'Blessing req'],
                    ['th', { props: { colspan: '2' } }, 'Blessing res'],
                ],
                ['tr',
                    ['th.col-id', 'CID'],
                    ['th.col-id', 'MID'],
                    ['th.col-name', 'Name'],
                    ['th.col-group', 'Group'],
                    ['th.col-data', 'Data'],
                    ['th.col-rank', 'Rank'],
                    ['th.col-blessing-req-token', 'Token'],
                    ['th.col-blessing-req-timestamp', 'Timestamp'],
                    ['th.col-blessing-res-token', 'Token'],
                    ['th.col-blessing-res-ready', 'Ready']
                ]
            ],
            ['tbody', components.map(({cid, mid, name, group, data, rank,
                                       blessing_req_token,
                                       blessing_req_timestamp,
                                       blessing_res_token,
                                       blessing_res_ready}) =>
                ['tr',
                    ['td.col-id', String(cid)],
                    ['td.col-id', String(mid)],
                    ['td.col-name', name || ''],
                    ['td.col-group', group || ''],
                    ['td.col-data', data || ''],
                    ['td.col-rank', String(rank)],
                    ['td.col-blessing-req-token', (blessing_req_token !== null
                                                   ? String(blessing_req_token)
                                                   : '')],
                    ['td.col-blessing-req-timestamp', (blessing_req_timestamp !== null
                                                       ? String(blessing_req_timestamp)
                                                       : '')],
                    ['td.col-blessing-res-token', (blessing_res_token !== null
                                                   ? String(blessing_res_token)
                                                   : '')],
                    ['td.col-blessing-res-ready', String(blessing_res_ready)]
                ]
            )]
        ]
    ];
}


window.addEventListener('load', main);
window.r = r;
window.u = u;
