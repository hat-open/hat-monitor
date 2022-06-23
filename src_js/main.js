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
                    ['td.col-rank-control',
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
                    ['th', { attrs: { colspan: '2' } }, 'Blessing req'],
                    ['th', { attrs: { colspan: '2' } }, 'Blessing res'],
                ],
                ['tr',
                    ['th.col-id', 'CID'],
                    ['th.col-id', 'MID'],
                    ['th.col-name', 'Name'],
                    ['th.col-group', 'Group'],
                    ['th.col-data', 'Data'],
                    ['th.col-rank', 'Rank'],
                    ['th.col-token', 'Token'],
                    ['th.col-timestamp', 'Timestamp'],
                    ['th.col-token', 'Token'],
                    ['th.col-ready', 'Ready']
                ]
            ],
            ['tbody', components.map(({cid, mid, name, group, data, rank,
                                       blessing_req, blessing_res}) =>
                ['tr',
                    ['td.col-id', String(cid)],
                    ['td.col-id', String(mid)],
                    ['td.col-name', name || ''],
                    ['td.col-group', group || ''],
                    ['td.col-data', JSON.stringify(data)],
                    ['td.col-rank', String(rank)],
                    ['td.col-token', (blessing_req.token !== null
                                      ? String(blessing_req.token)
                                      : '')],
                    ['td.col-timestamp', (blessing_req.timestamp !== null
                                          ? formatTs(blessing_req.timestamp)
                                          : '')],
                    ['td.col-token', (blessing_res.token !== null
                                      ? String(blessing_res.token)
                                      : '')],
                    ['td.col-ready', (blessing_res.ready
                                      ? ['span.fa.fa-check']
                                      : ['span.fa.fa-times'])]
                ]
            )]
        ]
    ];
}


function formatTs(timestamp) {
    const d = new Date(timestamp * 1000);
    const z = n => String(n).padStart(2, '0');
    const zz = n => String(n).padStart(3, '0');

    return `${d.getFullYear()}-${z(d.getMonth()+1)}-${z(d.getDate())} ` +
        `${z(d.getHours())}:${z(d.getMinutes())}:${z(d.getSeconds())}` +
        `.${zz(d.getMilliseconds())}`;
}



window.addEventListener('load', main);
window.r = r;
window.u = u;
