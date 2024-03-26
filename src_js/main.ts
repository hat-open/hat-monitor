import r from '@hat-open/renderer';
import * as u from '@hat-open/util';
import * as juggler from '@hat-open/juggler';


type LocalComponent = {
    cid: number,
    name: string | null,
    group: string | null,
    data: u.JData,
    rank: number
};

type GlobalComponent = LocalComponent & {
    mid: number,
    blessing_req: {
        token: number | null,
        timestamp: number | null
    },
    blessing_res: {
        token: number | null,
        ready: boolean
    }
};


const defaultState = {
    remote: null
};


let app: juggler.Application | null = null;


function main() {
    const root = document.body.appendChild(document.createElement('div'));
    r.init(root, defaultState, vt);
    app = new juggler.Application('remote');
}


async function setRank(cid: number, rank: number) {
    if (app == null)
        return;
    await app.send('set_rank', {
        cid: cid,
        rank: rank
    });
}


function vt(): u.VNode {
    if (!r.get('remote'))
        return  ['div.monitor'];
    return ['div.monitor',
        localComponentsVt(),
        globalComponentsVt()
    ];
}


function localComponentsVt(): u.VNode {
    const components = r.get('remote', 'local_components') as LocalComponent[];
    return ['div',
        ['h1', 'Local components'],
        ['table',
            ['thead',
                ['tr',
                    ['th.col-id', 'CID'],
                    ['th.col-name', 'Name'],
                    ['th.col-group', 'Group'],
                    ['th.col-data', 'Data'],
                    ['th.col-rank-control', 'Rank']
                ]
            ],
            ['tbody', components.map(({cid, name, group, data, rank}) => {
                name = name || '';
                group = group || '';
                data = JSON.stringify(data);
                return ['tr',
                    ['td.col-id', String(cid)],
                    ['td.col-name', {
                        props: {
                            title: name
                        }},
                        name
                    ],
                    ['td.col-group', {
                        props: {
                            title: group
                        }},
                        group
                    ],
                    ['td.col-data', {
                        props: {
                            title: data
                        }},
                        data
                    ],
                    ['td.col-rank-control',
                        ['div',
                            ['button', {
                                on: {
                                    click: () => setRank(cid, rank - 1)
                                }},
                                icon('go-previous')
                            ],
                            ['div', String(rank)],
                            ['button', {
                                on: {
                                    click: () => setRank(cid, rank + 1)
                                }},
                                icon('go-next')
                            ]
                        ]
                    ]
                ];
            })]
        ]
    ];
}


function globalComponentsVt(): u.VNode {
    const components = r.get('remote', 'global_components') as GlobalComponent[];
    return ['div',
        ['h1', 'Global components'],
        ['table',
            ['thead',
                ['tr.hidden',
                    ['th.col-id'],
                    ['th.col-id'],
                    ['th.col-name'],
                    ['th.col-group'],
                    ['th.col-data'],
                    ['th.col-rank'],
                    ['th.col-token'],
                    ['th.col-timestamp'],
                    ['th.col-token'],
                    ['th.col-ready']
                ],
                ['tr',
                    ['th.col-id'],
                    ['th.col-id'],
                    ['th.col-name'],
                    ['th.col-group'],
                    ['th.col-data'],
                    ['th.col-rank'],
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
            ['tbody', components.map(({
                cid, mid, name, group, data, rank, blessing_req, blessing_res}) => {
                name = name || '';
                group = group || '';
                data = JSON.stringify(data);
                return ['tr',
                    ['td.col-id', String(cid)],
                    ['td.col-id', String(mid)],
                    ['td.col-name', {
                        props: {
                            title: name
                        }},
                        name
                    ],
                    ['td.col-group', {
                        props: {
                            title: group
                        }},
                        group
                    ],
                    ['td.col-data', {
                        props: {
                            title: data
                        }},
                        data
                    ],
                    ['td.col-rank', String(rank)],
                    ['td.col-token', (blessing_req.token != null ?
                        String(blessing_req.token) :
                        ''
                    )],
                    ['td.col-timestamp', (blessing_req.timestamp != null ?
                        u.timestampToLocalString(blessing_req.timestamp) :
                        ''
                    )],
                    ['td.col-token', (blessing_res.token != null ?
                        String(blessing_res.token) :
                        ''
                    )],
                    ['td.col-ready', (blessing_res.ready ?
                        icon('selection-checked') :
                        icon('process-stop')
                    )]
                ];
            })]
        ]
    ];
}


function icon(name: string): u.VNode {
    return ['img.icon', {
        props: {
            src: `icons/${name}.svg`
        }
    }];
}


window.addEventListener('load', main);
(window as any).r = r;
(window as any).u = u;
